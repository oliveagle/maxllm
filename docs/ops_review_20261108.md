# MaxLLM 网关运行审计（2026-11-08）

> 范围：NAS 生产实例（审计起于 v0.3.8，收口于 v0.3.10），`/v1/responses` 家族路由 + auto/auto_home 双池。
> 数据快照：2026-11-08 13:30 CST（recordings 覆盖 11-04 04:00Z – 11-08 05:30Z）。
> 数据源：网关 access log（4 天，10.5k+ 行）、recording JSONL（6.5k 条 client_response）、
> Prometheus `/metrics`、SQLite `maxllm.db`、OpenTelemetry collector → OpenObserve
> （20 条 maxllm 指标流，15s 抓取）。
> 所有 IP / 主机名 / 密钥已脱敏。复现命令见附录 A。

## 0. 结论摘要

| # | 结论 | 证据 | 严重度 |
|---|------|------|--------|
| 1 | ~~上游流被截断仍按 200 计成功~~ **（已订正，见 §1.1）**：真正的问题是"200 + error 非空" 把两类完全不同的东西记成了同一件事 | 817 条 200 带 error，其中 **816 条是客户端断开**（`ErrorSource::Downstream`），**只有 1 条是上游截断**（`… without the termination chunk`）。客户端断开里 638 条集中在 11-07 12–18Z 事故窗口、一台工作站；27 条是 300s 客户端超时；窗口外仅 0.4%–5% | P0（口径） |
| 2 | ~~auto 池路由的**流式**请求全部丢失 usage，token/成本账本对主流量不可用~~ **✅ 已收口（2026-11-09 07:2x CST 复核）**：`request_logs` 尾部 3 000 行 `tokens_out>0` = **2 996**；`spend_logs` 0 行是因为 `virtual_keys`/`teams` 各 0 行 —— 按 key 记账的账本（`budget.rs` 只在虚拟 key 路径 `record_spend`）无对象可记，master key 直用时设计如此 | 原证据保留：`/v1/responses/auto` 4,257 个响应里 4,208 个（98.9%）tokens=0；全站 65.9% 的 200 响应零 usage | P0（已修） |
| 3 | ~~成本表为空，`cost_usd` 恒为 0，成本告警/预算失效~~ **✅ 已收口（2026-11-09 07:2x CST 复核）**：尾部 3 000 行 `cost_usd>0` = **2 905**、合计 **$19.52**（volc_t_1 $8.44 / volc_1 $5.23 / xiaomi $4.46 / go_2 $1.38）；部署后尾部 2 MB 日志 `no cost entry` **0 条**；`ollama_local` cost=0 是内置表「Zero-marginal-cost channels」的**显式 0/0 行**（`deepseek-v4.1-flash:cloud`，预付/自托管不按 token 计），非缺价 | 原证据保留：配置无 `[cost*]` 段；6,535 条完成日志 cost=0（仅 42 条非 0）；2,186 条 `no cost entry found` WARN（占全部 WARN 的 76%） | P0（已修） |
| 4 | ~~监控链路"只采不看"：无 maxllm 看板、无告警；日志全部灌进 `default` 流~~ **大部分收口**：maxllm 看板已建（§14.7，缓存命中率面板带活动量下限）、7 条告警规则已上线；**`stream-name` 已核（2026-11-09 08:2x CST，见 §14.17）**：NAS `otel-collector-maxllm` 服务 `active`，live config 与 ops 仓 `ops/machines/oleNas01/otel-collector-maxllm/config.yaml` md5 一致（`76274d36…`），导出器已按 `stream-name: maxllm_logs` / `maxllm_recordings` 分流，OpenObserve 实测两流 37 821 / 112 752 docs 且 `_timestamp` 正在推进；**残余仅 1 项** = 告警出口仍是 httpbin 占位（§12.7 待办②，需用户 webhook URL） | 原证据保留：OpenObserve 20 条指标流均在更新，但 0 条 alert、2 个 dashboard 均与 maxllm 无关；collector 单 exporter 写死 `stream-name: default` | P1（残余待用户 webhook） |
| 5 | ~~provider 改名造成身份断裂：指标里 38 条 breaker series vs 配置 30 个 provider；旧路由被前缀匹配静默吞掉~~ **✅ 已收口（2026-11-09 08:2x CST 三条实测，见 §14.17）**：`/metrics` 里 `one_api` series **0 行**；breaker DB `provider like '%one_api%'` **0 行**（全表 30 行 = 配置 provider 数）；旧 base_url 不再被静默吞掉 —— `POST /v1/responses/one_api_volc_1` / `one_api_zhipu_1` 均 **404** 且报错点名 `unknown provider or pool 'one_api_volc_1' …`，不再落到通用路由被错误 provider 服务 | 原证据保留：9 条 `one_api_*` 僵尸 series；`one_api_volc_2` 仍 force-open 到 2026-11-27；旧 base_url `/v1/responses/one_api_*` 落到通用路由并被错误 provider 服务 | P1（已修） |
| 6 | **body 哨兵 `auto` 在 `/v1/responses` 上仍然不可用**（真 bug，2026-11-08 23:1x 定位并修复）：`verbatim` 分支让 body 逐字节透传，`adopt_sentinel_model()` 只能解析、不能改写，字面量 `auto` 直接打到上游 | recordings 全量：失败上游尝试里 `model="auto"` **1 931 次**（`ollama_local 404×367`、`opencode_go_1 400×255`、`opencode_go_2 400×241`、`xiaomi 400×200`、`volc_1 404×189`、`minimax 400×118`）；`request_logs` 看不到，是因为 failover 之后成功那一行不带 `auto` | P1（已修 `c845b45`，见 §13.8） |
| 7 | ~~`/health` 太浅、池健康检查未接线~~ **✅ 已收口**：`/health` 现在回 `pools{healthy,total}` + `open_providers`/`half_open_providers`；`maxllm-pool-health.timer` 5 min 一次；quota probe 已按改名后的 `kimi` 运行 | — | P1 |
| 8 | ~~breaker 指标是惰性快照~~ **✅ 已收口**：`/metrics` 在 scrape 时输出 `effective_state()`，过期的 open 会算成 closed | `is_open()` 在下次请求时才 lazy half-open，gauge 直接读原子 state | P2（已修） |
| 9 | ~~`maxllm.err` 长期未轮转~~ **✅ 已收口**：`rotate_logs.py` 的 `DEFAULT_LOGS` 早含它，timer 每日跑（阈值 20 MiB / 留 2 代），当前 0 字节；sentinel 文案随 §6 一起消失 | 23 行、全部停留在 11-04；`route '/v1/responses/auto' references unknown provider 'auto'` | P2（已修） |
| 10 | ~~自检探针流量计入生产指标~~ **✅ 已收口（2026-11-09 08:2x CST 实测，见 §14.17）**：watch 巡检 infer / resp 两个 HTTP 探针已带 `X-MaxLLM-Probe: 1`，网关按 `ctx.rs` 语义「正常服务但排除出 metrics / request_logs / 熔断记账」；08:21:08 手动轮七项全 PASS，同时刻 `request_logs` 00:20:40–00:21:40Z 窗口 13 行**全是 6.8 万–23.6 万 token 的真实流量**，探针那两笔（≈90 tok / ≈38 tok）一行缺席 ⇒ 排除生效；旁证：`request_logs` 自 `2026-11-08T20:00Z` 起 `status>=400` **0 条** | 原证据保留：本机 loopback 探针命中已删除路由，产生 429/404/400 并写入 request_logs | P2（已修） |
| 11 | **`/v1/responses/<provider>` 只校验不钉住**（2026-11-08 晚发现并修复）：路径里的 provider 段被丢弃，请求按池路由走，最终由**别的** provider 作答，`route_path` 还记成 `/v1/responses` | 修复前 `/v1/responses/open_design` 实际由 `opencode_go_1` 作答；见 §12.1 | P1（已修 `ebcfa9f`） |
| 12 | **provider 改名不迁移熔断 / 会话状态**（§5.3，本轮修复）：旧行留在 DB，新行从 0 开始，下次启动 prune 还把旧行删掉 | 改名当天 `volc_1` 的 9 次 quota-open 归零、`volc_2` 的 force-open 被"解除"；实测 `one_api_volc_1`→`volc_1` 合并后 state/failures/force_open_until/quota_opens 全部保留 | P1（新增 `maxllm admin rename-provider`，见 §13.1） |
| 13 | **P2 三项按证据关掉，没有加机制**：请求体落盘（写入侧已脱敏 + 14 天保留）、`maxllm.err` 轮转（本就存在）、breaker gauge 语义（scrape 时算 effective state） | recordings 30 分片 84,833 行 `--dry-run` 0 行待擦；`maxllm.err` 0 字节 | P2（见 §13.2 / §13.3 / §8） |

**正常项**（不用改）：session 亲和命中 3,488 / miss 22（size 8）；97.6% 的客户端请求
以 200 结束；failover 链在 429/404 上工作正常；auto/auto_home 双池互不越界；OTel
指标采集 15s 稳定落库。

---

## 1. P0-1：流被截断，但按 200 计成功

> ⚠️ **本节结论已订正**：这 720 条里 816 条是**客户端断开**而不是上游截断，实测只有 1 条真截断。
> 原判据（`error` 非空）不可用；保留原文是为了留下订正轨迹，**以 §1.1 为准**。

**现象**：`client_response` 记录里带 `error` 字段的有 837 条，其中 **720 条 status=200**。

| provider | 200 且带 transport error | 错误形态 |
|---|---|---|
| ollama 系端点 | 679 | `Downstream ConnectionClosed … Prematurely before response body is complete` |
| 小米云 | 28 | 同上（对端 TLS 连接提前关闭） |
| 其他 | 13 | 连接被重置 / 超时 |

- ollama 系端点整体截断率：**680 / 1,981 = 34.3%**；11-07 12:00–19:00 事故窗口内
  每小时 30–62%。11-08 已回落到 4–8%，但仍在发生。
- 截断请求的 prompt 中位数 **132k tokens**（p90 206k），时长中位数仅 2.6s —— 上游
  先回 200 再立刻断开，典型的超大 prompt / 上下文上限行为。
- 代价：这些请求合计 **87.2M input tokens** 已被上游计费，客户端只拿到半截回答。

**为什么监控看不见**：网关只在 upstream 状态行上做 failover；响应体传到一半断开时
既不重试、也不把该请求标记为失败，`requests_total{status="200"}` 照常 +1。

**建议**（按性价比排序）：
1. 新增 `maxllm_stream_truncated_total{provider}` 计数器；client_response 增加
   `status="truncated"` 维度，成功率先按"含终止事件"重算。
2. 在 SSE 结束时校验终止事件（`response.completed` / `[DONE]`）：缺失则在关闭前补一个
   `event: error`，让客户端明确失败而不是静默接受半截答案。
3. 在"尚未向客户端写出任何字节"时允许 failover（本批 680 例里有 55 例 ttft==duration，
   可直接救回；更大的收益要靠第 1/2 条把问题暴露出来）。
4. 路由侧：`/v1/responses` 的主 provider 当前是 ollama 端点，而它是截断率最高的一家；
   把稳定 provider 提到 primary（或让 codex 客户端直接走 `/v1/responses/auto`）可立刻
   把 40% 的错误率降下来。**这是配置改动，需要确认成本/配额口径后再动。**

**证据**：
```
client_response: n=1904  /v1/responses/responses  200=1818  transport_error=767 (40.3%)
client_response: n=4257  /v1/responses/auto/...    200=4252  transport_error=31  (0.7%)
```

### 1.1 订正（2026-11-08 15:5x）：`Downstream ConnectionClosed` 是**客户端断开**，不是上游截断

§1 把 679 条 `Downstream ConnectionClosed … Prematurely before response body is complete`
读成了"上游先回 200 再立刻断开"。这个读法是错的。本轮用三步钉死了它的真实含义：

**① 错误语义**：pingora 的 `ErrorSource` 只有三种取值 —— `Upstream`（远端服务器）、
`Downstream`（远端客户端）、`Internal`。`Downstream ConnectionClosed … Prematurely before
response body is complete` 产生于 `pingora-core-0.8.0/src/protocols/http/v1/server.rs:938-952`
的 `read_body_or_idle()`：网关边写响应体边 poll 客户端套接字，**客户端先挂断**才会走到这里。

**② 消息里的 `Peer: addr: 10.126.126.4:11434` 是干扰项**：那是 `error_while_proxy()` 给
*每一个*代理错误挂的上下文

```rust
e = e.more_context(format!("Peer: {}", peer));
```

它打印的是上游 `HttpPeer`，与"哪一端先断开"无关 —— 上一轮正是被这一行误导成
"地址是上游 ⇒ 断的是上游"。

**③ 复现验证**：对一个流式请求读到首字节后 1 秒主动关闭客户端，网关落盘的是**逐字节相同**的一行
（`status_code=200`、`ttft_ms=598`、`duration_ms=1664`、
`error="Downstream ConnectionClosed context: Peer: addr: 10.126.126.4:11434 … Prematurely before
response body is complete"`）。

**重分类（4 天 recordings，9 010 条 200 响应；快照 2026-11-08 16:05 CST，可用 `scripts/stream_outcomes.py --dir recordings` 随时重算）**

| 类别 | 条数 | 判据 |
|---|---|---|
| 200 且 `error` 非空 | 817 | |
| ├ `Downstream ConnectionClosed`（**客户端断开**） | **816** | `ErrorSource::Downstream` |
| └ `Upstream ConnectionClosed … without the termination chunk, read 503 855 bytes`（**上游截断**） | **1** | `ErrorSource::Upstream` |

816 条客户端断开里 **814 条已经有 `ttft_ms`**（响应头 + 首包字节都已送达客户端），只有 2 条在
首字节前断开。它们由三个互不相干的群体组成，不是一种现象：

| 群体 | 条数 | 特征 | 读法 |
|---|---|---|---|
| 11-07 12Z–18Z 事故窗口内本网段一台工作站 | 638 | 逐小时放弃率 46.6% / 61.5% / 36.5% / 40.0% / 27.5% / 8.4%；放弃时刻中位 3s 内、相邻两条间隔中位 5.6s | 同一小时该端点上另有 315 条**完整成功**（tokens_out 100% 非零、ttft p50 1.22s）⇒ 上游健康，是客户端在反复取消/重来 |
| 另一台工作站打 `xiaomi/mimo-v2.6-flash` | 27 | 时长全部 301.5–305.7s ≈ ttft(1.5–5.7s) + **300s** | 客户端 300s 请求超时；模型确实慢，但与网关无关 |
| 其余零散 | 151 | volc_1 36 / xiaomi 43 / minimax 3 / deepseek 2 … | agent 循环里的中断、切模型、重试，属正常行为 |

窗口外放弃率只有 0.4%–5%（今天 07Z 是 20/1016 = 2.0%）。

两个群体各自的根因都查下去了：

- **27 条 300s 超时不是网关问题，是模型本身慢到贴边**。`xiaomi` / `one_api_xiaomi` 的成功请求
  延迟分布是 p50 18–21s、p90 82–93s、p99 119–240s、**最大 297s**，而**没有任何一条成功请求超过
  300s** —— 客户端 300s 一断，正好切在分布的尾巴上。`mimo-v2.6-flash` 仍在 `[[auto_model.pool]]`
  里，所以 auto 池里约 1/10 的请求要等 90s 以上，1% 会接近 5 分钟。**是否把它移出池子或给客户端
  调大超时是成本/交互口径决策**，本轮只留证据（见附录 B"仍未做"）。
- **638 条事故窗口内的放弃来自本网段一台工作站**，且同一小时该端点另有 315 条完整成功
  （tokens_out 全部非零、ttft p50 1.22s）⇒ 上游健康，是客户端在取消/重来；这轮没有再往下追
  "谁在取消"，因为它属于客户端行为、不是网关缺陷。

**因此 §1 的下列结论撤回或改写**

| 原文 | 订正 |
|---|---|
| "6 409 个 200 里 720 个（11.2%）带 transport error = 静默截断" | 只有 **1** 条是上游截断；816 条是客户端断开 |
| "ollama 端点整体截断率 34.3%，是截断率最高的一家，建议把 primary 换掉" | **反了**。按 `request_logs` 全量口径，`ollama_local` 的上游失败率 **0.00%**、`one_api_ollama_local` **0.11%**，是全仓最低档；真正高的是 `opencode_go_2` 7.65%、`opencode_go_1` 2.58%、`ada` 2.50%。34.3% 是"客户端放弃率"，且集中在 11-07 12–18Z 窗口 |
| "上游先回 200 再立刻断开，典型的超大 prompt / 上下文上限行为" | 不成立：被放弃请求的 ttft p50 **1.27s** 比成功请求的 **1.72s** 还快，首包已正常到达 |
| "截断请求 prompt 中位 132k ⇒ 上下文上限" | 132k 这个数是真的（那批行 `tokens_in` 中位 133 827），但它只说明"长 prompt 的请求更容易被客户端放弃"（想得久 ⇒ 更容易被中断），不能推出上游因上下文上限断开 |
| "87.2M input tokens 已被上游计费，客户端只拿到半截" | 输入 token 确实计费（下游断开行合计 **100.7M** input tokens，772/839 行有 `tokens_in`），但性质是"客户端放弃时已经产生的输入"，与"截断"无关；输出侧上游按其实际生成计 |

**对修复计划的影响**

1. **"首字节前 failover" 的收益从"救回 680 例"缩到 2 例（0.25%）** —— 不值得为它引入首包缓冲与
   交互延迟。该项从"待做"降为**不做**。
2. **`/v1/responses` primary 从 ollama 换掉** 的主要理由（"它截断率最高"）消失，ollama 端点的
   上游可靠性反而是全仓最好的。仍可因**配额/成本**换，但不再是可靠性问题。
3. `streams_judged_total` 的口径本来就对：只统计"网关能解析方言 + 上游给出终态或确认截断"的流，
   客户端断开天然不进入。代码在 `error_while_proxy()` 里用 `e.esource() == ErrorSource::Upstream`
   守着 `mark_stream_truncated()`，**客户端断开不会触发流截断降级** —— 否则任何一次 Ctrl-C 都能把
   健康 provider 熔断。线上 311 judged / 0 truncated / 0 次降级印证。
4. **口径分离已落地**：新增 `maxllm_client_aborted_total{provider}` —— `Downstream` 错误且
   尚未见到终止事件时 +1（`stream_terminal_seen` 守着"客户端恰好在终态事件发出的瞬间挂断"
   的竞态，那笔交换已经完成，不算放弃），这类请求不再进 `usage_missing_total`（token 本来
   就不会再来，算"provider 没报 usage"是找错人），也不进熔断/失败率。线上以主动断开的
   客户端验证：`client_aborted_total{provider="ollama_local"} 1`，且 `stream_truncated_total`
   与 `streams_judged_total` 均未被它影响。

### 1.2 订正（2026-11-08 19:5x CST）：4xx/5xx 被误记为 `stream truncated`

`request_logs` 里 2026-11-08 08:43 那条 `deepseek /v1/responses status=400
error="stream truncated: upstream ended the body before a terminal event"`
是**记账口径错误**：那是 deepseek 上游正常返回的一个 400（同分钟前后还有 6 条
error=None 的 400），不是"200 被截断"。

**根因**：`deepseek` 是 `native_responses = true`。流式 Responses 请求撞上 4xx 时，
JSON 错误体走 native Responses **流式**直传分支（`if ctx.is_streaming`），该分支没有
4xx 守卫 —— 而翻译路径（`if ctx.upstream_status >= 400` 提前返回）和 chat 路径都有。
于是 `observe_stream_chunk` 把 JSON 错误体当 SSE 扫：找不到 `response.completed`
终止事件 → `mark_stream_truncated`。后果有两层：

1. **日志/指标误导**：`logging()` 里 `truncation_error` 无条件拼进 `error` 字段，
   "stream truncated" 盖掉了真实错误；`maxllm_stream_truncated_total` 对 4xx 照样 +1，
   会让截断率口径被上游的常规 4xx 污染。
2. **客户端读到被污染的 body**：`observe_stream_chunk` 还在 JSON 错误体末尾**追加**
   `event: error
data: {…gateway_stream_truncated…}`，把 400/422 的 JSON 响应拼成了
   半个 SSE（实测 `temperature: 9` → 400、`max_output_tokens: -1` → 422 均复现）。

**修法（4 处，同一不变量：只有 2xx..399 的流才谈得上"截断"）**：

| 位置 | 改动 |
|---|---|
| native Responses 流式分支 | `if ctx.is_streaming` → `if ctx.is_streaming && ctx.upstream_status < 400`，4xx 改走下方缓冲/规范化错误体路径（与翻译路径一致） |
| `mark_stream_truncated()` | 入口加 `if ctx.upstream_status >= 400 { return false; }`（计数器唯一的记账点，单点兜底） |
| `observe_stream_chunk()` | 提前返回条件加 `ctx.upstream_status >= 400`（不再往错误体追加 SSE 事件） |
| `logging()` 的 `truncated` | `… && ctx.upstream_status < 400`（日志/指标双保险，即便未来有路径误标也保持诚实） |

**验证（线上部署后逐项复测）**：

| 检查 | 修复前 | 修复后 |
|---|---|---|
| `temperature: 9`（400）响应体 | JSON + 追加的 `gateway_stream_truncated` SSE 事件（~200B 冗余） | 纯 JSON 217B，`python3 -c json.load` 通过 |
| `max_output_tokens: -1`（422）响应体 | 同上被污染 | 纯 JSON 288B |
| `request_logs.error` | `"stream truncated: …"`（掩盖真实原因） | `None`（真实原因在响应体里） |
| `maxllm_stream_truncated_total{provider="deepseek"}` | 两次 4xx 后 = 2 | 重启后发同样的两个 4xx，series **不出现**（未记账） |
| 正常 200 流式 | — | 10039B 完整收尾，`streams_judged_total{deepseek}=1`、`stream_truncated_total` 仍为 0（无回归） |

新增单测 `an_error_response_is_never_booked_as_a_truncated_stream`：400 流无终止事件
→ 不标 truncated、body 原样、`mark_stream_truncated` 返回 false。unit 7/7、
integration 132/132、fmt / clippy `-D warnings` 全绿。

---

## 2. P0-2：auto 池的流式请求丢失 usage（token/成本账本失效）

**现象**：`/v1/responses/auto/responses` 共 4,257 个响应，**4,208 个（98.9%）tokens_in/out = 0**；
今天该路径 2,111 个响应中只有 3 个有 token —— 而且这 3 个正是手工发的
**非流式**请求。其余全站 200 响应中零 usage 占 **65.9%**。

**对照实验**（同一 provider、同一 model、同一 `stream:true`）：

| 请求 | 结果 |
|---|---|
| `POST /v1/responses/volc_1`（直连路由） | 200，SSE，记录 **85/17 tokens** |
| `POST /v1/responses/auto`（池路由，命中 volc_1） | 200，记录 **0/0 tokens** |
| `POST /v1/responses/auto` `stream:false` | 200，记录 **86/44 tokens** |
| `POST /v1/responses/ollama_local`（直连） | 200，SSE，记录 **32/20 tokens** |

**根因（已定位到代码路径）**：池路由必须把 body 的 `model` 从哨兵改写成池条目模型，
因此 `upstream_request_filter` 判定 `verbatim=false`、不设置
`keep_request_content_length`（`crates/maxllm-gateway/src/gateway.rs:2281-2301`）。
于是 `request_body_filter` 跳过了解析 `stream` 字段并安装 translator 的那个分支
（`gateway.rs:2439` 的 `if ctx.keep_request_content_length` 链）。落到后面的
"normal mode" 时，native Responses 直接复用 `ctx.is_streaming`（此时仍为 false）
作为 `TranslatedRequest.is_streaming`（`gateway.rs:3028-3031`），既不解析 body 的
`stream`，也不安装 `NativePassthroughStream`。

结果：响应按**非流式**分支处理，`extract_stream_usage()` 从不被调用
（它只在 translator 返回字节时执行，`gateway.rs:3814-3830`）。实测同时能看到副作用：
**客户端请求 `stream:true`，响应头却是 `Content-Type: application/json`，body 仍是 SSE**
（`event: response.created` …）。也就是说协议层也不一致，只是 SSE 客户端通常能容忍。

**建议**：
1. 在 auto 改写 body 的那段（`gateway.rs:2631-2665`，已经 `serde_json::from_slice` 过一次）
   顺手读取 `stream` 并设置 `ctx.is_streaming`；或在 normal mode 的 native 分支里解析一次。
2. 安装 translator 时对 `native_responses` 显式使用 `NativePassthroughStream`，避免
   `gateway.rs:3040-3056` 用 provider 的 chat translator 覆盖前面装好的 passthrough。
3. 回归测试：`auto_model=true` + `endpoint_type="responses"` + native provider +
   `stream:true`，断言 (a) 响应 `Content-Type: text/event-stream`，(b) `tokens_in/out` 非 0。
   现有 `test_auto_model_rotates_over_the_pool_and_rewrites_the_model` 只覆盖
   `/v1/chat/completions` 非流式；`test_opencode_streaming_usage_is_recorded` 只覆盖直连路由。
4. 修好前，任何基于 `/metrics` 的 token/成本报表都只能代表约 34% 的流量。

### 2.1 修复后实测（2026-11-08 17:0x CST）：usage 覆盖率 0% → 100%

§2 的三个建议在 `2cdb156` 落地并部署（14:06–14:23 CST 滚动重启）。用 `request_logs`
按 10 分钟切片复算，修复前后是断崖式的两段：

| 窗口（UTC） | 200 数 | 带 usage | 备注 |
|---|---|---|---|
| 03:00–04:09 | 285 | **0** | 只有 `one_api_*` 在服务，全灭 |
| 04:10–06:19 | 1 968 | 780（40%） | 混合：`ollama_local` 74% 有，`volc_1` 只有 9% |
| 06:20–06:29 | 208 | **208（100%）** | 14:23 CST 新二进制 |
| 06:50–08:29 | 1 508 | 1 507 | 之后一直满 |

**当日 `tokens_in=0 && tokens_out=0` 的 200 共 2 785 条，其中 2 778 条 `error` 为空**
（即不是客户端断开），几乎全部落在 06:20 之前 —— 这是 §2 那个 bug 的历史存量，不是
新故障；`2cdb156` 之后不再产生。**不要**拿"零 usage 比例"当作新的可靠性信号，除非
把窗口限定在该次部署之后。

剩余的少数零 usage 行（例：17:0x 前 2 小时内 4 条）逐条查过，**4/4 都带
`Downstream ConnectionClosed …`**，即客户端在 token 到达前就断开（`ollama_local` 3 + `minimax` 1），
属 §1.1 的客户端放弃，已由 `client_aborted_total` 与 `usage_missing_total` 的排除规则
分开记账，不是提取缺口。

### 2.2 `request_logs` 里的幽灵行：provider 为空、status=0（已修）

排查"2 小时内 6 条 `status=0` 且 provider 为空"的行时定位到一条真实缺陷。这类行的形状
完全一致：`provider=""`、`model=""`、`latency_ms=0`、`route_path=""`、`client_ip=NULL`、
`endpoint_type=ChatCompletions`（`EndpointType::default()`），历史累计 88 条（10-31 起，
常成簇出现，间隔 5–200 ms）。

**根因**：`request_filter` 的四个快路径（`/health`、`/version`、`/v1/models`、`/metrics`）
把 `ctx.skip_logging = true` 放在 `write_response_header/body(...).await?` **之后**。
客户端在网关写响应时重置连接（监控挂断、端口探测、浏览器重载）→ 写操作返回
`WriteError … Connection reset by peer` → `?` 直接把错误抛出 `request_filter`，
`skip_logging` 仍是 `false` → `logging` 钩子照常记账：写一条 provider 为空的
`request_logs`、给 `requests_total{provider="none",model="unknown",status="0"}` +1、
并对**从未自增过**的 `active_requests` 做一次 `dec`（快路径在 `inc` 之前就返回了），
所以监控挂断得越多，`active_requests` 漂得越负。

**复现**（线上现役二进制，100% 命中）：对 `/health` 发一个请求后立刻用
`SO_LINGER=0` 关闭 → 每次都新增一行幽灵行；13/13 次探测（health/metrics/models/version）
全部命中。修复后在同样探测下：0 行、`active_requests` 保持 0。

**修复**（`fix/phantom-request-rows`）：
1. 四个快路径把 `skip_logging = true` 挪到第一个 `await` 之前；
2. `logging` 里 `active_requests.dec()` 改为只在 `ctx.active_request_counted`（真正
   `inc` 过）时执行，杜绝负值漂移；
3. `logging` 增加 `is_unrouted_session()` 守卫：既没有 `route_index` 也没有
   `provider_name` 的会话（从未到达路由层）不再写 `request_logs`、不再算作
   `requests_total{status="0"}`，改记新指标 `maxllm_unrouted_sessions_total`；
   其中 `ErrorSource::Downstream`（客户端挂断）降为 `debug`，其余（内部缺陷）保持 `warn`；
4. `request_path` 在 `request_filter` 一进来就落进 ctx，未路由的会话也能在日志里看到路径。

**测试**：集成测试 `aborted_fast_path_probe_leaves_no_request_log`（四个快路径各做一次
RST 探测 → 断言 `request_logs` 为空、`active_requests==0`；再发一个真实
`/v1/chat/completions` → 断言恰好 1 行且 `provider=="mock_openai"`、`status==200`，
证明守卫没有把正常请求一起吞掉）；单元测试 `unrouted_session_predicate_needs_both_a_route_and_a_provider`
钉住判据。**该集成测试在修复前必然失败**（实测断言输出 4 条幽灵行），修复后通过。

**运维口径**：历史上那 88 条幽灵行保留原样（审计数据不改写），统计时用
`WHERE provider <> ''` 过滤；`status=0` 的真实含义应从这一行起只看"有 provider"的行。

---

## 3. P0-3：成本账本整体不可用

- `maxllm.toml` 中没有任何成本表配置 → `calculate_cost()` 全部落到
  `no cost entry found, returning 0`（4 天内 2,186 条 WARN，占日志 WARN 的 76%）。
- 完成日志里 `cost_usd="0.000000"` 共 6,308 条，只有 42 条非 0（opencode 自带 cost）。
- SQLite `spend_logs` 表 **0 行** → 预算 / 花费 / 告警功能实际从未工作。
- 叠加 P0-2 后，即使配置了价格表，池路由的流式流量仍会按 0 token 记账。

**建议**：补价格表（至少 6 家云 provider），或明确关闭 cost 模块并删掉 WARN 噪声；
同时把 usage 缺失率做成指标（`maxllm_usage_missing_total`）。

### 3.1 价格表复核（2026-11-08 17:0x CST）：单价对得上，但口径还缺缓存

`2cdb156` 之后账本已有数。以开销最大的 `minimax`（`MiniMax-M3`）为例，取 6 小时窗口：

| 项 | 值 |
|---|---|
| 请求数 | 1 290 |
| `tokens_in` | 119 229 901 |
| `tokens_out` | 508 589 |
| 记账成本 | **$36.379** |
| 反推单价 | $0.3051 / 1M input |

用内置价格表（`crates/maxllm-admin/src/costs.rs`：MiniMax-M3 = $0.30/M in + $1.20/M out）
复算：119.230 × 0.30 + 0.5086 × 1.20 = **$36.377**，与落库值一致 ⇒ 没有重复计费、
没有单位错位。单价本身也与厂商公开价一致（M3 标准档 $0.30 / $1.20，prompt < 512k；
超过 512k 翻倍）。

**本轮已补齐**：
1. **prompt cache 折价（已完成）**。M3 支持自动 prompt 缓存，缓存命中的 input 在第三方
   渠道约 $0.05/M；实测命中率 99.5%+（下文 `cached_input_tokens_total` 对
   `tokens_in_total`）。价格表新增可选的 `cached_input_per_1m`
   （`[model_costs."…"]` 每行）：有值时按
   `(tokens_in − cached) × list + cached × cache_rate + out × out_rate` 计价，`cached`
   钳制到 `tokens_in`（防上游超报或解析下溢导致负费）。未配置该字段的模型维持现状：
   全部按 list —— 这是**刻意**的，渠道缓存单价未知时宁可按 list 高记，也不低估账单。
   `CostCalculator` 内置 `MiniMax-M3 → $0.05`（exact 优先、glob 兜底，配置可覆盖），
   据此 minimax 单请求记账从 ~$0.055 降到 ~$0.010 量级（99.5% input 按 $0.05 计）。
2. **>512k 分档翻倍**未实现（当前 6 小时均值 92k/请求，暂未触发）——仍是本轮遗留，
   但账本已按真实缓存价记账，$36 不再是上限而是含缓存折价后的口径。

**已做（采集半）**：`copy_usage_tokens`/`extract_usage` 现在解析三种缓存字段
（`prompt_tokens_details.cached_tokens` / `input_tokens_details.cached_tokens` /
Anthropic `cache_read_input_tokens`），落在 `ctx.cached_tokens_in`，暴露为
`maxllm_cached_input_tokens_total{provider,model}`（是 `tokens_in_total` 的**子集**，
不是额外量），完成日志也带 `cached_in`。两条解析路径原来各抄一份，现在合流成
`copy_usage_tokens`，单测钉住"三条形态 + 缓冲/流式同值"。

**记账侧已上线**：`gateway` 的 `logging` 钩子改调
`cost_calculator.calculate_cost_with_cache(…, ctx.cached_tokens_in)`，`build_hot_state`
按配置逐模型 `set_cache_rate`；`calculate_cost()` 的旧签名保留（内部转调、cached=0），
对外接口不变。单测：`cached_input_is_billed_at_the_cache_rate` /
`cached_tokens_are_clamped_to_the_input_count` / `cache_rate_override_and_glob`。

**上线验证（2026-11-08 17:2x CST，二进制 `72005e5e…`，`NRestarts=0`）**：
- 重启后 `maxllm_cached_input_tokens_total` / `maxllm_unrouted_sessions_total` 两项新指标
  均出现，后者恒为 0；`maxllm_active_requests` 正常回落。
- 命中率（重启以来累计）：`MiniMax-M3/minimax` **98.55%**、`mimo-v2.6-flash/xiaomi`
  89.47%、`deepseek-v4-flash/volc_1` 81.41%、`opencode_go_2` 64.29%、`opencode_go_1`
  52.86%。M3 的公开缓存价与"命中率 ~99%"的假设成立。
- 成本（同一批 16 条 `minimax` 请求，新二进制）：`tokens_in` 1 294 769 / `tokens_out` 9 737，
  记账 **$0.1002**；同样 token 全按 list 价应为 **$0.4001** ⇒ 该批折价 **4.0×**。
  单条逐字核对：60 170 in / 68 out，其中 59 804 命中 ⇒ 366×0.30 + 59 804×0.05 +
  68×1.20 = **$0.0031816**，与落库值完全一致（旧口径 $0.01813，5.7×）。
- 幽灵行回归：重启后 `provider=""` 与 `status=0` 行均为 **0**；journal 无 error/panic。
- 主线回归：`open_design` / `cmd` 路由仍解析（401 鉴权而非 404 unknown provider），
  说明 rebase 到 `59ea47f` 后并发会话的 open-design 工作未回退。

---

## 4. P1：可观测性"只采不看"

- OTel collector 正常：20 条 `maxllm_*` 指标流 15s 更新，最新时间戳与当前一致。
- 但 OpenObserve 中 **0 条 alert**、2 个 dashboard（LiteLLM LB / cross_ex_arb），
  **没有 maxllm 看板**。也就是说：指标进了库，但没有任何人/规则会看。
- 日志管线把 recordings + maxllm.log 全部写进写死的 `stream-name: default`
  （该流已有约 6,000 万条文档），无法按来源过滤，查询成本高且相互污染。

**建议**：
1. collector 拆两个 exporter（或两个 pipeline 各带不同 `stream-name`），至少区分
   `maxllm_logs` / `maxllm_recordings`。
2. 建 maxllm 看板：QPS、成功率（按 provider/route）、p50/p95、fallback 率、
   breaker open 数、零 usage 比例、stream truncated 比例。
3. 至少 4 条告警：5xx/4xx 比例、truncated 率 > 1%、零 usage 比例 > 5%、
   breaker open > 2 providers 持续 10 分钟。

**收口（2026-11-08 22:1x–22:4x CST）**：三条都已落地，而且发现并修掉了**看板/告警本身算错**
的一处（见 §12.7）。

| 建议 | 现状 |
|---|---|
| 日志分流 | ✅ `stream-name: maxllm_logs` / `maxllm_recordings` 两个 exporter；指标按 OpenObserve 的命名规则天然一股一表（45 条 `maxllm_*` stream） |
| 看板 | ✅ `MaxLLM Gateway — reliability & cost` 21 个面板（§12.7），16 条声明式面板逐条实测：10 条有数、6 条为空（截断/放弃/5xx/零 usage 当前本来就是 0）、**0 条报错** |
| 告警 | ✅ 7 条（5xx 比例、4xx 比例、breaker open、truncation 率、零 usage 率、首字节前截断、录音脱敏、流降级），全部 `enabled` 且已挂 `maxllm-http-sink`；**出口仍是占位的 `httpbin.org`，要真告警得给 webhook**（§12.7 末） |
| 探针 | ✅ `maxllm-pool-health.timer` 在跑（5 min）；探针带 `X-MaxLLM-Probe: 1`，不进指标/请求日志 |

---

## 5. P1：provider 改名留下的三处断裂

1. **指标僵尸 series**：`/metrics` 里 38 个 breaker provider 标签，配置只有 30 个
   provider；9 条是 `one_api_*` 旧名。它们来自进程内旧标签缓存 + DB 历史行。
   `one_api_volc_2` 的 `force_open_until` 停在 **2026-11-27 23:59:59 +08**，
   会让看板长期误报"2 家 provider down"。
2. **旧路由被前缀匹配静默吞掉**：`match_route` 是 `path.starts_with(route.path)`。
   改名前客户端 base_url `/v1/responses/one_api_volc_1` 现在会命中通用
   `/v1/responses` 路由，被换成 ollama 端点 + 10 家 fallback，然后把模型名
   `x`/旧名发给上游，产生一批 404/400（当天 7 次，全部来自本机探针）。
   建议：对 `/v1/responses/<name>` 这类"一 provider 一路由"的路径做**精确匹配**，
   未知 provider 直接 404 并点名。
3. **改名不迁移 breaker/配额状态**：旧行留在 DB，新行从 0 开始。改名当天
   `volc_1` 的 9 次 quota-open 历史归零，`volc_2` 的 force-open 也被"解除"，
   等于把熔断器的记忆重置了。建议 provider rename 时按 `(base_url, model)` 迁移
   状态行，或提供 `maxllm admin rename-provider` 子命令。

   **✅ 已修（2026-11-08 22:5x–23:0x CST）**：新增 `maxllm admin rename-provider`
   （`AdminStore::rename_provider` + `POST /admin/providers/rename`），在**一个 SQLite
   事务**里把 `circuit_breaker_state` 迁到新名、把 `session_bindings` 的 provider 列改写。
   目标名已有行时按 `CircuitBreakerStateRow::merge_stricter` 合并（开着的仍开、较晚的
   force-open deadline 胜出、计数器取 max 不相加）。`--dry-run` 走**只读连接**
   （`SqliteStore::open_read_only`，不碰正在写 request_logs 的生产库）。命令先校验配置
   顺序：`--to` 必须已在配置里、`--from` 必须已不在配置里，否则报错并提示「先改配置」——
   因为启动时的 prune 会把迁错的行动删掉。配套改动：启动 prune 遇到**有实质状态**
   （state≠0 / failures>0 / quota_opens>0 / 未过期 force-open）的未知 provider 行时
   打 WARN 并点名该命令，不再静默计数。测试 +10（in-memory / sqlite / 重开文件库 /
   API 端点 / 输入校验），workspace **845 全绿**，fmt+clippy 干净。实测：在 NAS 生产库
   的副本上把 `one_api_volc_1`(state=1, failures=4, quota_opens=3) 迁到已有行的
   `volc_1` → 合并结果为 `state=1/failures=4/force_open_until=1794153600000/quota_opens=3`，
   两条 session pin 一并改写；真库上 `--dry-run` 只读可用。

---

## 6. P1：body 哨兵 `auto` 仍然不可用（✅ 已修，见 §13.8）

- `/v1/responses`（非池路由）收到 `{"model":"auto"}` 时，body 里的哨兵会被原样发给
  primary provider → 404，然后沿 fallback 链一路 404/400。
- 当天仍有 11 次 404 + 2 次上游 400 由它触发；历史 425 次 404 集中在 11-07 12:00–14:00。
- 设计上不给 `/v1/responses` 加 `auto_model=true` 是为了避免把显式模型静默改写
  （这个判断是对的）。**建议改成精确匹配**：只有当 body `model` 恰好等于某个池名
  （`auto` / `auto_home`）时才走池；其他模型名保持原样。这样既保留显式模型语义，
  又消除哨兵的 404 风暴。

**❌ 订正（2026-11-08 23:1x CST）**：上一段「`request_logs` 里 `model='auto'` 最后一次是
2026-11-07T14:35Z、所以已不再触发」是**看错了数据源** —— `request_logs` 记的是 failover
之后成功那一行，上游收到字面量 `auto` 的**失败**尝试只留在 recordings 里。按 recordings
重查：失败上游尝试 `model="auto"` 共 **1 931 次**，带哨兵的客户端请求 `/v1/responses` 128 次、
`/v1/responses/responses` 30 次。

**✅ 已修（`c845b45`，2026-11-08 23:1x 定位、23:2x 部署两台）**：根因是 `verbatim` 分支把
body 按 Content-Length 逐字节透传，改写就会截断 —— 只要选中的 provider 是任一 auto 池的
成员，就改成 chunked 帧让哨兵被改写成该 entry 的真实模型。这样不需要 §6 原来提的
「精确匹配池名」方案（那会改变路由契约）：显式模型语义本来就由 `adopt_sentinel_model()`
只认池名来保证。详见 §13.8。

---

## 7. P1：健康检查与探针

- `/health` 只返回 `{"status":"ok"}`，不反映上游池健康；`/version` GET 返回 405。
- `pool_health.py` 已写好但没有 systemd timer / 常驻进程 → auto 池的 11 家云厂商
  健康度只能手工看。
- `maxllm-quota-probe-kimi` 单元仍用改名前的 `--provider one_api_kimi`（当前配置只有
  `kimi`）；能跑是运气，改名后配置解析若变严格就会静默失效。
- 本机 loopback 探针产生的 429/404 会计入 `request_logs` 与 `/metrics`，污染成功率。

**建议**：`/health` 增加 `pools` 摘要（每池 healthy/total/最小剩余额度）；
pool_health 接 timer（5 min）；probe 显式传 `--provider kimi`；探针请求带
`X-MaxLLM-Probe: 1` 并在指标中排除。

### 7.1 修复（2026-11-08 18:1x CST）：熔断器"半开探针槽"泄漏 —— 13 个健康成员被永久 503

**现象**：`pool_health.py` 报 25 个池成员里 16 个 `503 "All providers are unavailable
(circuit breakers open)"`、耗时 0–1 ms（根本没碰上游），而同一时刻 `/health` 把它们
全部列进 `half_open_providers`（语义是"等一次探测"）。其中包括 `ollama_local` ——
本轮订正过的上游失败率 0.00% 的那家。

**根因**：`CircuitBreaker::is_open()` 既是**判定**又是**状态机驱动**：fail window
结束后第一次被调用时，它把 `Closed→HalfOpen` 并 `pending_probes += 1`，占掉该 provider
唯一的探测槽。而所有**过滤**路径（`ProviderSelector::select` 的候选遍历、
`AutoPoolState::usable_entries`、`switch_to_fallback` 的池/链遍历）都对**每个**候选调用
`is_open()`，于是被"看过但没被选中"的 provider 各占一个槽、永不释放
（`pending_probes` 只在 HALF_OPEN 的 `record_success` / `record_http_failure` 里
`fetch_sub`，而且**不落盘**）。之后它对任何请求都读作"开闸"→ 503，直到重启。
12 个本地 GPU 端点 + `ollama_local` 就是这样被自己的路由读操作打死的。

**修法**（`crates/maxllm-gateway/src/{circuit_breaker,routing,gateway}.rs`）：

1. 新增**纯读** `CircuitBreaker::allows_traffic()`（`effective_state() != STATE_OPEN`，
   不写任何原子量），所有过滤/判定路径改用它；
2. `is_open()` 只在**最终被选中的那个 provider** 上调用一次（`ProviderSelector::select`
   末尾、`select_from_auto_pool` / `next_fallback_provider` 的选取处）作为"准入 + 占槽"；
   槽被并发探测占用时顺延到下一个健康候选，而不是让整条路由 503；
3. 新增幂等、钳零的 `release_probe()`；`record_success` / `record_http_failure` 用它归还，
   `logging()` 在请求终态无条件归还（覆盖探针流量、4xx failover、客户端中途断开这些
   "不记账"路径）；
4. 新增兜底 `reap_stale_probe()`：槽被占超过 2×`fail_timeout`（下限 120 s）视为泄漏并
   回收 —— 任何遗漏的释放路径最多阻塞 120 s，而不是永久；
5. `Closed→HalfOpen` 改用 `compare_exchange`（与 `Open→HalfOpen` 一致）；429 / 配额
   强制开闸 / 流截断降级 / `restore()` 都顺手清空探测槽。

**验证**（线上部署 `e2c87f4e…`，`NRestarts=0`，journal 0 error/panic）：

| 指标 | 修复前 | 修复后 |
|---|---|---|
| `pool_health.py` BAD 503 | 16 / 25 | **4 / 25**（全部是 `kimi`/`volc_2`/`volc_t_1`/`zhipu_1` 的配额强制开闸，正确） |
| `ollama_local` 定向请求 | 503 circuit open（0 ms） | 200，随后 `record_success` 关闭熔断（`/health` 的半开名单 −1） |
| 本地 GPU 死端点 | 503 circuit open（把真实故障掩盖成"熔断"） | 502 `Upstream ConnectionClosed`（如实暴露） |
| 新增回归测试 | — | `test_inspection_does_not_consume_a_probe_slot`（25 次纯读不占槽）、`test_a_stale_probe_slot_is_reclaimed`、`test_selection_only_reserves_the_chosen_providers_probe_slot` |

注意：`auto_home` 那 11 个成员 502 是**上游真的没在跑**（家里 GPU 服务未启动），
不是本缺陷；修复只是让它们从"被误报成 503 熔断"变成"如实报 502"。

### 7.2 修复（2026-11-08 18:5x CST）：异构 fallback 链的模型名改写 + 诚实 503

两个在 §7.1 复核中暴露的旁路缺陷，一起修（commit `aa79acd`，已上线验证）；
同日晚些时候补上了 404 分支（commit `0569de3`，见 C）：

**A. §828 设计缺口：模型名改写（真缺陷）**

- 现象：通用 `/v1/responses` 路由 primary=`ollama_local`（模型 `deepseek-v4.1-flash:cloud`），
  fallback 链 10 家云渠道各持不同模型名。客户端把 primary 专属名原样发给链上每家：
  volc/minimax/deepseek 等都不认，答 400 `unknown model`；`should_failover_status` 因
  `default_model != ctx.model` 判为"可换"，于是整条链把 400 走完，**primary 一挂该模型名整条路由即死**。
- 修法（**只在 400/422 且该名真的属于本部署某 provider 时**触发，retry 同一 provider 一次）：
  1. `request_body_filter` 新增 `model_override` 通道：重试时把 body 的 `model` 字段改写为目标
     provider 的 `default_model`，再走翻译/转发；
  2. `upstream_request_filter` 把 `model_override.is_some()` 并入 `is_auto_route` ——
     改写后的 body 体积会变，必须走 chunked，不能保留原 Content-Length 逐字节直传
     （**踩坑**：native Responses 直传分支不改 body，第一版上线后 minimax/deepseek 仍收到原名，
     靠 recordings 逐条核对才发现 retry 的 `ctx.model` 已对、body 没对）；
  3. 三道闸：`no_later_candidate_carries_model`（还有一家声称持有该名的没试过 → 先 failover 不改写，
     避免静默换模型）、`model_belongs_to_some_provider`（拼错的名字不改写，保持 400）、
     `model_rewritten_providers`（每 provider 至多改写一次，防 ping-pong）。
- 新指标 `maxllm_model_rewrite_retries_total{provider}`。
- 验证（线上真实上游）：`/v1/responses/minimax` 与 `/v1/responses/deepseek` 发
  `deepseek-v4.1-flash:cloud` → **修复前 400 unknown model，修复后 200**（各自改写为
  `MiniMax-M3` / `deepseek-flash`）；`volc_1` 本就认该名，200 不变。
- 5 条新回归测试（含 native Responses 路径 + 改写的"最后手段"闸门 + 拼错名保持 400）。

**B. 诚实 503：不再谎报 "All providers are unavailable"**

- 现象：`pool_health` 的 503 文案统一是 `All providers are unavailable (circuit breakers open)`，
  而它探的是**钉死的单 provider 路由**（`/v1/responses/<provider>`）——一家配额 force-open，
  文案却让运维去找路由 bug（§7.1 的 16/25 一度就是这样被误读的）。
- 修法：`select_provider` 空结果时 `unavailable_reason()` 逐候选点名：
  `no provider available: kimi=circuit open (quota force-open) — every candidate of route '/v1/responses/kimi' is rejecting traffic`。
- 验证：线上 `/v1/responses/{zhipu_1,volc_t_1}` → 503 点名 + 原因；`pool_health` 最新一轮 4 家
  配额 503 全部自解释，11 家 502 如实暴露。

**C. 404「模型不存在」分支（真缺陷，同日补充）**

- 现象：A 的改写只覆盖 `400 | 422`。实测 vLLM / miniCPM 上游对未知模型不答 400，而是 **404**：
  `{"error":{"message":"The model deepseek-v4.1-flash:cloud does not exist. Valid aliases: minicpmv47-awq."}}`。
  而 404 在 `should_failover_status` 里是硬编码的"可 failover"，于是请求直接换 provider / 耗尽链路，
  改写永不触发 —— 两条本地 GPU 的 Responses 契约（`olefedora_gpu0_minicpmv47`、
  `coding_home_olefedora_gpu0`）从此整条 404。
- 难点：404 本身不区分"模型不存在"与"端点缺失"（两者同为 404），不能无脑改写。
- 修法（延迟决策，复用 429 的 pending 形状）：
  1. header 阶段 `status == 404` 且**改写确实可行**（`model_rewrite_target` 的闸门全过）时置
     `pending_model_404`，把决策推到 body 完整之后 —— pingora 此时尚未向客户端写出任何字节，
     重试对客户端不可见；
  2. `body_reports_unknown_model()` 判定 body 是否在说模型：强特征（`unknown model`、
     `model not found`、`model does not exist`、`does not exist. valid aliases`、`invalid model` …）
     直接命中；弱特征（`does not exist`、`not found`、`不存在`）必须同时提到 model/模型 ——
     `404 page not found` 这类端点缺失不会误判；
  3. 命中 → 走与 400/422 **完全相同**的三道闸，并在同一 provider 原地改写重试
     （`begin_model_rewrite` 统一两处的状态复位）；不命中 → 按普通 404 换链，**模型名原样**带给下游；
  4. 每 provider 至多一次（复用 `model_rewritten_providers`）：二次 404 原样返回客户端，不会 ping-pong。
- 顺带收敛：429 延迟决策与 404 共用的换链尾部合并为 `fail_over_from_body_filter`
  （回放上限 / failover 预算 / 审计事件 / `switch_to_fallback` / 指标一处实现），行为不变。
- 验证（线上）：两条 Responses 契约发该名 → **修复前 404，修复后 200**
  （响应体 `"model":"minicpmv47-awq"`）；发拼错的名字 → 仍 404 且不改写；
  `maxllm_model_rewrite_retries_total{provider}` 各 +1；journal 0 error。
- 测试：4 条单测钉 body 分类器（vLLM 别名体 / one-api 体 / 端点 404 / 弱特征需提到 model）；
  3 条集成测试钉 404 改写成功、非模型 404 换链且模型名不变、二次 404 只改写一次后返回客户端。

---

## 8. P2：小项

| 项 | 现状 | 建议 |
|---|---|---|
| breaker gauge 语义 | **✅ 已收口**：`/metrics` 在 scrape 时输出 `effective_state()`（惰性快照里 fail window 已过的 open 会被算成 closed） | — |
| `maxllm.err` | **✅ 已收口**：`rotate_logs.py` 的 `DEFAULT_LOGS` 已含 `maxllm.err`，`maxllm-logrotate.timer` 每日 00:00 跑（`--max-bytes 20MiB --keep 2`）；文件当前 0 字节（11-04 的 3.4K 已归档为 `.err.20261104-archive`）。`unknown provider 'auto'` 文案来自 §6 的哨兵分支，已随 `adopt_sentinel_model` 收口 | — |
| 请求体落盘 | **✅ 已收口**：`[recording] redact = true` + `redact_pii = true` 在**写入时**脱敏。实测：30 个分片 / 84,833 行里 `redact_recordings.py --dry-run`（含 PII 规则）判定 **0 行需要擦**；唯一命中 `secret` 的两行是一轮脱敏自测留下的 `[REDACTED]`/`[EMAIL]` 占位，说明写入侧规则真的生效。保留期由 `maxllm-logrotate.timer` 的 `--recordings-days 14` 执行 | — |
| `/metrics` 暴露 | **✅ 已收口**：0.3.9 起远程抓取一律 `403 metrics is loopback-only`（loopback 抓取不受影响，collector 本来就走 localhost）；两台节点均已实测 | — |
| `cmd` provider 403（2026-11-08 19:5x CST） | 直连上游 `api.commandcode.ai/provider/v1/{responses,chat/completions}` 均 403 `{"code":"upgrade_required","message":"Your Go plan doesn't include API access. Upgrade to Provider or higher"}` —— 是**订阅档位问题，不是网关 bug**（网关正确透传 403 JSON）。Go 档位能用的通道是 `cmd_cli`（CLI 桥，`maxllm-commandcode.service`） | 升级 Provider 档，或把 `/v1/responses/cmd` 路由指向 `cmd_cli`；`cmd` 的 403 由上游计费决定，网关侧无需改 |
| 大 prompt | p50 96k tokens、p90 200k（`request_logs.tokens_in`）；原写"≥128k 截断率 38.5%"用的是被误分类的客户端放弃行，按 `Upstream` 口径重算**没有证据**（§1.1） | 无证据支持预检；若要做，动机应是**上游成本/额度**（长 prompt 每次都是全额输入计费）而不是截断 |

---

## 9. 修复优先级

| 优先级 | 动作 | 类型 | 预估 |
|---|---|---|---|
| P0 | auto 池流式 usage 修复 + 回归测试（§2） | 代码 | 0.5–1 天 |
| P0 | truncated 计数 + 终止事件校验 + 可见的成功率（§1） | 代码 | 1 天 |
| P0 | 价格表或显式关闭 cost + 去 WARN 噪声（§3） | 配置/代码 | 0.5 天 |
| P1 | `/v1/responses` primary 换成稳定 provider（需确认成本口径） | 配置 | 10 分钟 |
| P1 | OpenObserve maxllm 看板 + 4 条告警 + 日志分流（§4） | 运维 | 0.5 天 |
| P1 | 路由精确匹配 + rename 状态迁移 + 清僵尸行（§5） | 代码 | 1 天 |
| P1 | body 哨兵精确匹配池名（§6） | 代码 | 0.5 天 |
| P2 | health 池摘要 / probe 排除 / 轮转 / 脱敏（§7–8） | 混合 | 0.5 天 |
| ~~P1~~ | ~~`client_aborted_total` + 把客户端放弃从成功率口径里摘出去（§1.1）~~ **✅ 已做**（见附录 B） | 代码 | — |

> **§1.1 订正后的两处降级**：`/v1/responses` primary 换 provider 不再是可靠性问题（ollama 端点
> 上游失败率 0.00%，全仓最低）；"首字节前 failover" 收益只有 0.25%，**不做**。真正要补的是
> 客户端放弃与网关/上游失败的**口径分离**（上表最后一行）。

---

## 10. 优化：auto 池 latency-aware 轮转（2026-11-08 19:5x–20:2x CST）

**问题**：auto 池按「计费分档 + 轮转」选 provider，同档内是纯 round-robin。
`xiaomi`（`mimo-v2.6-flash`，订阅制）因此拿到 ~1/7 的交互流量，但它的**整段
响应耗时**是同级最慢的：近 24h 200 响应里 p50 23.0s / p90 92.5s / p99 279.5s /
max 459.9s，而 minimax p50 3.2s、volc_1 4.4s、deepseek 2.2s。24h 内 auto 池
10 151 次请求中 9.7% 超过 30s，其中 **35.8% 的慢请求落在只占 8.6% 流量的
`xiaomi` 上**；27 条 300s 客户端超时也全部在它身上。

**关键订正**：xiaomi 的**首字延迟并不慢**（ttft p50 2.4s），慢的是**生成时长**
（整段响应 p50 23s）。所以路由信号必须用「整段响应 EWMA」，不能用 TTFB
（`provider_stats::latency_ms` 是 TTFB，喂的是 `LatencyBased` 路由策略）。

**实现**（commit `71b8a1f` + `19e7b69`）：
- `provider_stats` 新增 `record_total_latency` / `total_latency_ms`：整段
  交换耗时（请求开始→客户端最后字节）的 EWMA，成功请求才记账。
- auto 池轮转按「快队」（fast cohort）挑：同一计费档内，只保留**整段 EWMA
  ≤ max(档内中位数 × 2.5, 5s)** 的 entry；无测量值的 entry 保留（还没被证明慢，
  且轮转本身才会产生第一次测量）；全部无测量值时退回全档（刚启动=纯轮转）。
- 每 50 次轮转做一次 exploration，走全档，让恢复/新加的 provider 重新被测量。
- **sticky 钉住也走同一个过滤器**：session 钉在慢 provider 上时，钉不被 honour
  （记一次 affinity miss），请求落到快 provider 并**重新钉住**——和「钉在熔断 /
  低计费档 / 探针槽满」的处理一致。否则一个会话会被永久拖在慢 provider 上。
- 新指标 `maxllm_auto_pool_slow_skips_total{provider}`；
  配置开关 `[auto_model].prefer_fast`（默认 `true`，设 `false` 回到纯轮转）。
  **线上 maxllm.toml 未改动**（默认即开）。

**测试**：8 条单测（中位数/5s 地板、同质簇不误杀、全新无测量、exploration、
跳过计数、整段 EWMA）+ 1 条集成测试（3 provider，慢 mock 延迟 6s：会钉住慢
provider，测完两条快线后，钉该 session 的下一次请求必须落到快 provider，
慢 mock 只收到最开始 1 次请求）。`cargo fmt --check` / `clippy -D warnings` /
`cargo test --workspace`（unit 178 / integration 133）全绿。

**线上验证**（二进制 `0130293504…`，NRestarts=0）：
- 24+20 次 `/v1/responses/auto` 探针全 200，`xiaomi` 一次都没被选中；
  `maxllm_auto_pool_slow_skips_total{provider="xiaomi"}=23` 且只此一个 provider。
- 定向验证 session 重钉：一个 DB 里钉在 `xiaomi` 的会话
  （`01a1ba19-…`）发 `session-id` 请求 → 由 `minimax` 服务，affinity miss +1，
  绑定重写为 `minimax`（`updated_at` 刷新）。
- 预期收益：按 24h 对照（排除 xiaomi）auto 池 >30s 占比 ~9.7% → ~6.8%。
- **部署范围**：NAS 节点先上线；2026-11-08 21:5x CST 本机节点（第二台）也升级到同一 0.3.10 构建，两台一致（见 §12.5）。

---

## 11. open_design 入 auto 池（2026-11-08 20:3x–20:4x CST，纯配置）

**背景**：当天新接的三个 provider 里，`open_design`（`amr-link.open-design.ai`）实测 200，
且用户确认是**订阅制**（预付额度，边际成本 0）；`cmd` 在 Go 计划下恒 403；`cmd_cli` 是本机
CLI 桥接。三者都不在 `auto` 池里，`auto` 池仍是 11 条。

**入池条件（function calling 是硬门槛）**：池子要跑 codex 的工具调用，所以 provider 必须
真能出 `function_call`。实测：

- `POST /v1/responses/open-design` 带 `tools` → 返回 `function_call`（`get_weather`）✅
- `POST /v1/responses/cli_cmd` 带 `tools` → 上游回 `"tools":[]`，模型明说
  「I don't have a `get_weather` tool」，**没有 function calling** ❌

**落地（NAS `maxllm.toml`，纯配置、无新版本）**：池尾追加

```toml
[[auto_model.pool]]
provider = "open_design"
model = "deepseek-v4.1-flash"
```

`billing` 留空 = 订阅层（`AutoPoolEntry.billing` 的 default 是 `BillingMode::Subscription`），
因此排在 metered 的 `deepseek` **之前**；`cost_usd=0.000000` 是订阅制的正常表现，不是漏配价格。

流程（照既有部署口径）：备份 `maxllm.toml.bak-20261108-203923-open-design-pool` → 在 `:3799`
用改过 `listen` / `db_path` 的副本空跑（providers=33 / routes=70 /
`pool=auto entries=12 subscription=11 metered=1`、auto_home 14 条不变、池路由实测命中
`X-MaxLLM-Provider: open_design`）→ `os.replace` **同目录**原子替换（跨文件系统的
`os.replace` 会 EXDEV，所以先 stage 到目标目录再替换）→ 文件 watcher 热加载，日志
`Config reloaded successfully`。

**线上验证**：热加载后 14 发 `/v1/responses/auto` 探针 → open_design 命中 4 次，其余落在
volc_1 / minimax / ollama_local；`maxllm_auto_pool_metered_selections_total` 仍为 **0**
（没掉进 metered 层）。新 provider 没有测量值 → 进快队（`auto_pool_fast_cohort` 对无测量
entry 保留），所以它立刻拿到轮转份额；等第一批 EWMA 回来后由 §10 的过滤器决定去留。

> **注（2026-11-08 晚）**：`cmd_cli` 整条桥接已在 0.3.10 下线（厂商 TOS + 没有 function calling），见附录 C；下面这段保留为当时的判断依据。

**`cmd_cli` 暂不入池**：除了没有 function calling，还有两个代价——每次调用 ~24 963 input
token、每次起一个 CLI 进程，厂商称持续代理订阅违反 TOS。关键在于**网关侧目前没有
「provider 不支持 function calling ⇒ 不参与带 tools 的请求」这类过滤开关**：
`AutoPoolState::usable_entries` 的 `usable` 谓词只看熔断器状态
（`gateway.rs::select_from_auto_pool`），不看请求是否带 `tools`。硬入池会让 codex 的工具
调用**静默失效**——模型直接给文字答案、不再调工具，agent 循环看起来「正常结束」但什么都没干。
要入池得先加这个能力开关（provider 级 `supports_tools` + 池选路按请求特征过滤）。

---

## 12. 收口轮（2026-11-08 21:0x–22:1x CST）：一个真 bug、一个被忽略的 402、两次"不是 bug"的订正

这一轮把上一版遗留的三个疑点全部用数据结掉，其中 `/v1/responses/<provider>` 不钉住是**真 bug**（已修），
`402` 被当成"没发生"是**成本 bug**（已修），`Downstream` 记账与 `volc_1` 熔断则是**结论正确、不用改**。

### 12.1 `/v1/responses/<provider>` 只校验不钉住（真 bug，commit `ebcfa9f`）

**现象**（修复前线上实测）：

```
POST /v1/responses/open_design      ← 下划线；配置里的路由是 /v1/responses/open-design
```

这条路径落进 `[[routes]] path = "/v1/responses"` 的**前缀匹配**。`unknown_responses_subroute()`
只确认「`open_design` 是个已知 provider」，然后**把这一段丢掉**，请求继续按池路由走：
`ollama_local`(404) → `minimax`(400) → `xiaomi`(400) → `kimi`(429) → 最终由 `opencode_go_1` 作答。
即**路径点名的 provider 与实际作答的 provider 完全不同**，`request_logs.route_path` 还记成
`/v1/responses` —— 审计里看起来就是一次普通的池请求。任何拼写近似（下划线/连字符）都会静默降级成池路由。

**修复**（`crates/maxllm-gateway/src/`）：

| 位置 | 改动 |
|---|---|
| `ctx.rs` | 新增 `path_pinned_provider: Option<String>` |
| `gateway.rs::responses_subroute_pin()` | 紧邻 `unknown_responses_subroute()`：解析出 provider 段就设 pin，并把 `plugin_ctx.route_path` 改写成 `/v1/responses/<provider>` |
| `select_provider` | 有 pin 时**跳过 auto 池**，候选只放 pin 那一个，session-sticky 不参与 |
| `next_fallback_provider` | 有 pin 直接返回 `None` —— 失败就如实回报，不拿别家答案顶替 |
| `unavailable_reason` | pin 失败时点名 "the provider '<x>' named by the request path" |
| `no_later_candidate_carries_model` | pin 视为单 provider 路由（保留既有的 model 改写路径） |

**测试**：3 条集成测试（pin 生效且池零请求 / pin 失败不外溢 / 未知段仍 404），workspace 834 全绿；
`cargo fmt --check` + `clippy --workspace --all-targets -- -D warnings` 干净。

**修复后线上验证**：

```
POST /v1/responses/open_design        → 402 (2.1s)，日志 provider=open_design
                                        route_path=/v1/responses/open_design fallback_used=0
POST /v1/responses/volc_1             → 仍是诚实的 503（quota force-open 中）
POST /v1/responses/no_such_provider   → 仍是 404
POST /v1/responses                    → 池路由行为不变
```

### 12.2 `402 Payment Required` 被当成"什么都没发生"（成本 bug，commit `33a7599`）

**背景**：`open_design` 现在稳定返回 `402 {"code":"insufficient_balance"}`（钱包没钱，上游真去查了
余额，所以耗时 0.4–2.1s 而不是立刻回）。**402 既不是 5xx 也不是 429**，落不进任何记账分支 ⇒
熔断器永远认为它健康 ⇒ 每一轮选路都白付一次上游往返，然后才 failover 到别家。§11 把它加进
`auto` 池之后，这笔浪费变成**主流量路径上的固定开销**。

**修复**：

- `circuit_breaker.rs` 新增 `PAYMENT_REQUIRED_COOLDOWN_MS = 10 min` 与其文档（402 是账户级墙，
  不是瞬时故障：不进 `max_fails`，但必须立刻退出轮转，且余额充上后能自己回来）；
- `gateway.rs::upstream_response_filter` 新增 `payment_required = status == 402`，记账块条件扩为
  `(quota_open || payment_required || server_error || success)`，最前面加 `if payment_required`
  分支：`record_quota_until(now + 10min)` + `circuit_breaker_quota_opens_total` +1 + `unpin_session`
  + `info!`。10 分钟后 `is_open()` 自己清掉 force-open 并回 closed，届时充过值的 provider 会被
  下一次请求探活（不需要重启/重载配置）。

**测试**：`test_402_payment_required_force_opens_the_provider` —— 主 provider 回 402、fallback 回
200，**两次**客户端请求都必须 200 且主 provider 只收到 **1** 次上游调用；同时断言
`quota_opens=1`、`circuit_breaker_state=1`、`/health.open_providers` 含该 provider。
把 `let payment_required = status == 402;` 临时改成 `false` 重跑，该测试在 `quota_opens` 断言处变红
（`None != Some(1.0)`）—— 回归有保护。workspace 835 全绿。

**线上验证**（两台都打出来了）：

```
13:57:27Z INFO 402 payment required — force-opened until the balance is topped up
        provider=open_design cooldown_ms=600000        (NAS)
13:58:39Z INFO 402 payment required — force-opened until the balance is topped up
        provider=open_design cooldown_ms=600000        (本机节点)
/health.open_providers = ["open_design", "volc_t_1"]，auto 池 healthy 12 → 10/12
maxllm_circuit_breaker_quota_opens_total{provider="open_design"} 1
```

**仍未做**：余额没充之前，`open_design` 留在 `auto` 池里只是"每 10 分钟白打一次"，不再每轮白打。
是否**移出池**是成本/配置决策，等用户拍板（§12.6）。

### 12.3 `Downstream ConnectionClosed` 三臂实验：记账分类是对的，**不改**

上一版 §1.1 已把这条错误订正为"客户端断开"，但留了一个猜测："native Responses 直传结束得太突然，
客户端还没来得及读完 ⇒ 被误记"。三臂实验（NAS 本机对 0.3.10 打流式请求，逐个观察）：

| 客户端行为 | 网关表现 | `client_aborted_total` |
|---|---|---|
| 读到 `response.completed` 后继续 drain | 保持 keep-alive 2s 后正常 EOF | 不增 |
| 读到 `response.completed` 后**立刻** close | 不产生任何 `Downstream` 行、不增计数 | 不增 |
| 读到首条 `delta` 后立刻 close（mid-stream） | `error=Downstream ConnectionClosed…`、`tokens_in=tokens_out=0`、`status=200` | **+1** |

⇒ 那个"结束太突然被误记"的猜测**不成立**：终态事件是分界点，见到终态再挂断一分不记；只有真正
中途放弃才记。分类正确且公平，**这条链路不再改**。

顺带把"为什么 volc_1 的 Downstream 比例特别高"钉住了 —— 与会话自身的数据一致：本会话经网关打了
**326 次 volc_1，其中 175 次 Downstream（53.7%）**；同一 `client_ip` 下 `minimax` 1682 次只 1 次、
`opencode_go_2` 212 次 0 次。volc_1 按小时 09h 30/76 → 10h 29/61 → 11h 46/85 → 12h 70/119
单调上升。机制就是 native Responses 直传 + 客户端完成即挂断（pingora `read_body_or_idle()` 报
`Downstream`）。**归因是客户端行为，不该进失败率**；`ollama_local`（20/619）与 `open_design`
（41/152）偏高同理，是"长回答被 agent 循环主动掐断"，不是提供方故障。

### 12.4 `volc_1` 熔断核实：**是真配额耗尽，不是误熔断**

一度怀疑 `learned_pattern` 把普通 429 误判成"周额度"。核实结论是否定的：

- `circuit_breaker_state`：`volc_1` `quota_opens=2`，`force_open_until=1794153600000`
  = **2026-11-09 00:00 +0800**（周额度重置点）；
- 日志铁证：`2026-11-08T12:50:11.408979Z 429 quota force-opened until the resolved reset instant
  provider=volc_1 reset_at_ms=1794153600000 source=learned_pattern`，紧接
  `retrying on the fallback provider status=429 provider=minimax failed_provider=volc_1`；
- `rate_limit_patterns` 里确有该 learned 指纹（"已超过每周用量上限"模板）。

⇒ 上游确实说"本周额度用完"，重置时刻也是它自己给的。**不动它**，等 00:00 自动恢复（§12.6 有
恢复观察项）。同一时刻同为配额耗尽：`volc_2` / `volc_t_1`（到 11-09 00:00）、`zhipu_1`（到 11-10 22:58）。

### 12.5 部署状态

| 节点 | 二进制 sha256（前 12） | 版本 | 状态 |
|---|---|---|---|
| NAS 节点 | `c2375971da30` | 0.3.10 | active，`NRestarts=0`（2026-11-08 21:56:30 CST 起） |
| 本机节点 | `c2375971da30` | 0.3.10 | active，`NRestarts=0`（2026-11-08 21:57:29 CST 起） |

两台跑的是**同一个** musl 静态二进制（含 `ebcfa9f` + `33a7599`）。上一版为 `10ed814c6c28`（含
`ebcfa9f`，不含 402 修复）。镜像版本号仍是 0.3.10 —— 本轮不再单独 bump（`--version` 不能区分这两个
构建，用 sha256 认）。

**对外入口 VPS 仍是 0.3.8**，且是对外流量的主上游，尚未升级（需要 root 凭据，见 §12.6）。

### 12.6 本轮之后仍未做

| 项 | 状态 | 说明 |
|---|---|---|
| `open_design` 是否留在 `auto` 池 | **待用户拍板** | 402（余额为零）期间每 10 分钟仍白打一次；留在池里等用户充值，或移出池 |
| `volc_1` 恢复观察 | 待 00:00 | 确认 `quota_opens` 不再增长、`/health` 里 `volc_1` 消失 |
| 推送 `main` | ✅ **已推** | `0badb54..a039aef`（4 commit）；push 事件确实不触发 Actions，已用 `gh workflow run CI --ref main` 补跑，run `42487933112` 七项全绿 |
| 打 tag 发版 | **待用户拍板** | 远程 tag 只到 `v0.3.9` —— **`v0.3.10` 从未打 tag**，与上一轮记录的「tag v0.3.10 → 0badb54」不符；要发版需先 bump 0.3.11（或补打 tag） |
| VPS 升级 0.3.8 → 0.3.10 | **阻塞** | 需 root 私钥/密码；拿到后跑 `ops/machines/<vps>/maxllm/deploy.sh`（SOURCE=NAS） |

### 12.7 可观测性收口：看板/告警声明化，并修掉一处**跨节点重复计数**

**先纠正我自己的一次误判**：这轮开头用 `GET /dashboards/<id>` 数面板时读的是 `v8.panels`，得出「看板
0 个面板」；真实位置是 `v8.tabs[0].panels`，看板当时已有 12 个面板（脚本自己的计数是对的）。记在这里
是因为「看板是空壳」这个结论一度写进了本轮的中间判断。

**真 bug：同一个 provider 在 OpenObserve 里有两条 series。**

采集把两台网关节点（NAS + 本机）的 `/metrics` 都送进同一批 metric stream，靠 `service_name` 区分
（`maxllm-olenas01` / `maxllm-gateway`，新版 collector 另加 `node` 标签）。于是任何对 **gauge** 的
`sum by (provider)` / `sum(...)` 都在**把节点当实例重复计数**：

| 表达式 | 值 | 含义 |
|---|---|---|
| `sum(maxllm_circuit_breaker_state == 1)`（**修复前的告警**） | **10** | 10 条 (provider, 节点) 对 |
| `sum(max by (provider) (maxllm_circuit_breaker_state) == 1)` | **6** | 6 个不同 provider |
| 两台 `/health` 的并集 | 6 | `kimi` / `open_design` / `volc_1` / `volc_2` / `volc_t_1` / `zhipu_1` |

告警阈值是「> 2 providers」，所以**一个 provider 在两台节点同时熔断就会被记成 2**，接近误报；而面板
如果照抄 `sum by (provider)`，`2` 又会和状态码 `2 = HALF_OPEN` 撞车，读起来像「半开」。修法是先按
provider 折叠再判断：`sum(max by (provider) (maxllm_circuit_breaker_state) == 1)`，去重后与两台
`/health` 的并集**逐个对齐**（6 = 6）。告警与面板都已改成这个写法并重新 apply。

计数器（requests / tokens / fallbacks）跨节点相加是正确的（那就是机队总量），不需要去重；这次只动
gauge。

**看板/告警本身变成可重建的**（`scripts/openobserve_dashboard.py`）：

- 面板集从 12 → **21**：补上请求速率（按 provider）、5xx/4xx 比例、p95 延迟（
  `histogram_quantile(0.95, …)`）、输入/输出 token 速率、breaker open 数、half-open 数、quota-open
  次数、零 usage 比例、metered 池选中数；原来只有截断/放弃/脱敏那 5 条。
- 脚本从「只加不改」改成**声明式校正**：标题在本文件里的面板，若 query / stream / unit 与声明不符就
  改回来（正是这次修 breaker 面板所必需），未声明的面板（手工加的）一律不碰；空看板也能从内置
  skeleton 播种（原来没有可克隆的模板时脚本直接退出）。
- `--apply` 之后**逐面板回读校验** query + stream + unit —— 更新接口对「静默丢弃」的写入也回 200。

**实测**（16 条声明式面板逐条打 Prometheus 兼容接口）：10 条有数、6 条为空（截断/客户端放弃/5xx/
零 usage 当时本来就是 0）、**0 条报错**；重复执行 dry-run 输出 `nothing to add — 21 panels already
present`（幂等）。

**仍未做**：告警出口。7 条告警都 `enabled`、都挂在 `maxllm-http-sink` 上，但那个 destination 指向
`https://httpbin.org/post`（占位）。也就是说**会触发、会发请求、但不会通知到任何人**。替换只需一条命令：

```sh
OPENOBSERVE_ENDPOINT=<...> OPENOBSERVE_AUTH=<...> \
  python3 scripts/openobserve_alerts.py --apply --destination-url <飞书/钉钉/Slack webhook>
```

需要用户给一个 webhook（本机 dotvault 里没有飞书 bot 的 URL）。

---

## 13. 收尾轮（2026-11-08 22:5x–23:1x CST）：§5.3 落地，P2 三项按证据关掉

本轮没有新增线上事故，做的是「把上一轮记下的建议改掉或证明不必改」。

### 13.1 provider 改名的状态迁移（§5.3，已实现）

- `maxllm-admin` 新增 `AdminStore::rename_provider(from, to)`：一个 SQLite 事务里搬
  `circuit_breaker_state`、改 `session_bindings.provider`；目标名已有行时
  `merge_stricter`（state/failures/last_failure_at/force_open_until/transitions/
  quota_opens 取 max，successes_half_open 取 min，`updated_at` 取较新）。计数器**不相加**，
  免得同一对名字反复迁移把历史越滚越大。
- `maxllm admin rename-provider --from --to [--db] [--config] [--dry-run] [--json] [--force]`：
  离线跑（网关停着也行、`[admin] enabled = false` 也行）；`--dry-run` 用
  `SqliteStore::open_read_only()`，**不**在生产库上取写锁。
- 顺序护栏：先查配置，`--to` 不在配置里 / `--from` 还在配置里都直接报错（`--force` 可越过），
  因为启动 restore 会 prune 掉「provider 已不在配置里」的行 —— 迁早了等于白迁。
- 网关侧：启动 prune 碰到**有实质状态**的未知 provider 行（state≠0 或 failures>0 或
  quota_opens>0 或 force-open 未过期）不再静默删，改为 WARN 并点名该命令。
- 测试：+10 个（in-memory 迁移/合并/同名拒绝/无状态；sqlite 迁移/合并/重开库后仍在；
  API 端点成功与三种输入校验）。workspace **845 passed / 0 failed / 4 ignored**。
- 验证：NAS 生产库副本上 `one_api_volc_1`(open, failures=4, quota_opens=3,
  force_open_until=1794153600000) → `volc_1`（已有 closed 行），合并后四个字段全部保留，
  两条 session pin 改写；真库 `--dry-run` 只读跑通。

### 13.2 请求体落盘（§8）：写入侧已脱敏，历史分片无一需要擦

`[recording] redact = true` / `redact_pii = true` 在写入时生效，脚本只是给「开关之前」
的历史分片补刀。实测 NAS `recordings/` 30 个分片 84,833 行：`redact_recordings.py
--dry-run`（PII 规则开）**0 行待擦**。全库扫 `secret` 只命中 2 行，是一轮脱敏自测写进去的
`Ignore this test secret [REDACTED] and mail [EMAIL]` —— 反向证明写入侧规则在跑。
保留期已由 `maxllm-logrotate.timer`（`--recordings-days 14`）执行，无需再加机制。

### 13.3 `maxllm.err`（§8）：轮转早就在，只是文件是空的

`rotate_logs.py` 的 `DEFAULT_LOGS` 本来就含 `maxllm.err`，timer 每日 00:00 跑，
阈值 20 MiB / 保留 2 代；当前 `maxllm.err` 0 字节（11-04 的 3.4K 已手工归档为
`.err.20261104-archive`）。原来那条「无轮转」是**看文件时间戳得出的误判**，不是缺功能。

### 13.4 body 哨兵 `auto`（§6）：`request_logs` 看不到，真 bug 在上游侧

§6 当时的结论「已不再触发」是**看错了数据源**。`request_logs.model` 记的是网关最终选中的
provider 与模型；哨兵只有在**上游尝试**里才见得到，而上游 400/404 之后网关会 failover，
最后成功的那一行永远不会带 `auto` —— 所以「`request_logs` 里 `model='auto'` 停在 11-07」
只说明幽灵行（§2.2）没了，不说明哨兵被正确改写。

按 recordings 全量重查，真相反过来：**失败上游尝试里 `model="auto"` 共 1 931 次**，
top 是 `ollama_local 404×367`、`opencode_go_1 400×255`、`opencode_go_2 400×241`、
`xiaomi 400×200`、`volc_1 404×189`、`minimax 400×118`；带哨兵的客户端请求里
`/v1/responses`+`auto` 128 次、`/v1/responses/responses`+`auto` 30 次；
单链实例 `01a1bbad-18bc-7012-af66-732e2ef910f8` 是 ollama_local 404(model=auto) →
minimax 500 → opencode_go_2 200。根因与修复见 §13.8。

### 13.5 二进制备份堆叠（本轮顺手清掉）

部署习惯是 `cp -p maxllm-server maxllm-server.bak-<stamp>-<label>` 再换新的。一天下了
NAS 上堆到 **39 个 32 MB 的 `.bak-*`**（`~/opt/maxllm/bin` 657 MB，根分区 78%），
本机 5 个。已给 `rotate_logs.py` 加 `--bin-dir` / `--bin-keep`（默认留最新 5 个），
只轮转**带日期戳**的部署备份（`.bak-YYYYMMDD-*`）；手工命名的归档
（`.bak-pre-auto` / `.bak-pre-v020` / `.bak-pre-v032`）和 `.disabled-*` 一律不碰。
NAS 上实测清掉 31 个（657 MB → 156 MB），并把 `--bin-dir bin --bin-keep 5` 写进
`maxllm-logrotate.service` 的 ExecStart；本机 oleFedora01 原先**根本没有**这个 timer，
本轮补建了同样的 service+timer（每日 00:00 + 开机 10 min，`Persistent=true`）。
两个脚本（`rotate_logs.py` / `redact_recordings.py`）此前零测试，本轮补了
`scripts/tests/test_ops_scripts.py`（11 个用例：O_APPEND 语义下的 copy-then-truncate、
按龄清 recordings、按 mtime 留最新 N 个备份且不碰归档、脱敏键/value 哨兵/PII 可选/
未改动分片不动 mtime），并作为 CI 的 `Ops scripts` job 接线。

### 13.6 `open_design` 402 的实际节奏（观察，不改行为）

修复上线后 1 小时内 `402 payment required — force-opened` 出现 **6 次**
（13:57:27 / 14:07:39 / 14:18:19 / 14:30:16 / 14:40:47 / 14:51:39 UTC），
`circuit_breaker_state.quota_opens` 从 1 涨到 6，`force_open_until` 每次顺延 10 分钟。
即「每 ~10 分钟白打一次上游」，单次 0.4–2.1s、无 token 消耗 —— 代价是命中它的那条请求
多等一次往返再 failover，全天约 144 次。

没有加「指数退避」：402 是**余额耗尽**，恢复靠人工充值，10 分钟窗口正是为了充值后
无需重启即自动恢复；把窗口拉到 30–60 分钟会把恢复也拖慢。真正的选择是「留池等充值」还是
「移出池」（§12.6 第一条，待拍板）——移出池即归零，不必加机制。

### 13.7 仍未做（不变）

`open_design` 是否移出 auto 池、是否发版（bump 0.3.11 或补打 `v0.3.10` tag）、
告警 webhook 换真实出口、VPS 0.3.8 → 0.3.10 升级 —— 四项都需要用户拍板或凭据。

### 13.8 body 哨兵 `auto` 的真修复（`c845b45`，已部署两台）

**根因**：`upstream_request_filter` 的 `verbatim` 分支让 body 保持 Content-Length 逐字节
透传（native Responses 与 openai-kind 都会走），于是 `request_body_filter` 里的
`adopt_sentinel_model()` **只能解析、不能改写** —— 改了长度就会把 body 截断。字面量
`auto` 因此原样打到上游，404/400 → failover，每次还赔掉该会话的 prompt cache（上一条
每 turn 都要付，不只是慢一次）。

**修法**：只要选中的 provider **是任一 auto 池的成员**，就把 body 改成 chunked 帧
（auto 路由本来就是这么做的），哨兵才有机会被改写成这个 entry 自己的 model：

```rust
let provider_serves_a_pool = hot.auto_pools.iter()
    .any(|pool| pool.entry_for(&ctx.provider_name).is_some());
let may_carry_pool_sentinel =
    ctx.endpoint_type == EndpointType::Responses && provider_serves_a_pool;
let verbatim = !is_auto_route && !may_carry_pool_sentinel && (/* 原条件 */);
```

判据用「在池里」而不是「native」：`openai_style` 会把 chat 翻译的 provider 也判成
verbatim，1 931 次里相当一部分是这类。

**回归测试**：`test_body_sentinel_is_rewritten_for_a_native_responses_provider` ——
`native_responses = true` 的池成员承接 `POST /v1/responses {"model":"auto"}`，必须
**1 次**上游调用且 `model == "model-member"`；去掉 `!may_carry_pool_sentinel` 该测试立刻
变红（2 次调用、字面量 `auto` 被转发）。CI（`42493888653`，`c845b45`）七项全绿：
Format / Check / Clippy / Test / Responses policy / Ops scripts / Build Release。

**双机部署（2026-11-08 23:1x / 23:2x CST）**：musl release，sha256
`7de0f4b73637e4b68f05a329784e361cfcef7a2358b6967587ed6b0fc3de51ce`；两台
`--version` 0.3.10、`/health` status=ok、`NRestarts=0`、journal 无 panic。
实测 `POST /v1/responses/deepseek {"model":"auto"}`：本机 200 / `model=deepseek-flash` /
`fallback_used=0` / 646 ms，NAS 200 / `model=deepseek-flash` / 1.37 s —— 哨兵在到达上游
之前就已改写，白打的那一跳没有了。

---

## 14. 缓存命中率攻坚（2026-11-09 02:1x–03:xx CST）：opencode affinity 是主根因，A/B 两杠杆落地

用户三句话推进本轮：

> 「我怎么感觉速度有点儿慢呢？是因为没有充分利用 session sticky 么？缓存命中率如何呢？」
> →「给我优化缓存利用率，争取所有 provider 的缓存命中率 > 95%.」
> →「A+B 做掉. kimi 要保留。」

### 14.1 基线：三个主力 provider 掉在 95% 以下

窗口 `2026-11-08T09:30:00Z` 起、只统计带 `cached_in` 的 `request completed` 行
（脚本 `baseline.py`，12 158 行）：

| provider | tokens_in | 命中率 | 备注 |
|---|---:|---:|---|
| minimax | 779 727 116 | 97.39% | |
| open_design | 21 788 876 | 96.89% | |
| volc_t_1 | 24 999 360 | 96.46% | |
| ollama_local | 254 466 873 | 96.27% | |
| volc_1 | 153 108 633 | 95.60% | |
| **xiaomi** | 234 981 776 | **94.76%** | 差 0.24pp |
| **opencode_go_1** | 76 397 749 | **67.67%** | |
| **opencode_go_2** | 85 603 464 | **64.83%** | |
| cmd_cli | 334 424 | 48.38% | 一次性 CLI 桥接测试流量，15 reqs 全落在 11-08 10:18–12:59，不计入主力 |
| **合计** | **1 631 409 044** | **93.54%** | |

其余 deepseek / humanizer_gpu3 / coding_home_* 等 ≤176 token，全是首 turn 冷启动，不参与判定。

### 14.2 口径坑：`cached_in` 是 `1dda90d` 才加的字段

- `cached_in` 首次出现在 `2026-11-08T08:42:42Z`。**把缺失的 `cached_in` 当 0，会把 93.54% 误算成 70.1%**。
- 日志带 ANSI 色码，且 **`=` 两边也被色码包住**
  （`cached_in\x1b[0m\x1b[2m=\x1b[0m118208`）：
  - 先 `re.sub(r"\x1b\[[0-9;]*m", "", line)` 再匹配；
  - 判字段是否存在必须用 `"cached_in" in raw`，**不能**用 `"cached_in=" in raw`。
- 时间戳是 UTC（`Z`），用 `datetime.strptime(...).replace(tzinfo=timezone.utc)`；
  `time.mktime` 会差 8 小时。

### 14.3 opencode 掉到 65% 的真根因：网关不认客户端真正发的那个 session header

**修法前的识别集合**只有 `x-opencode-session`、`x-session-affinity`、`x-opencode-session-id`，
既不认 `x-session-id` / `thread-id`，也**没有任何 fallback** —— 解析不到就每请求铸一个全新 UUID。
于是 sticky pin 每次都是新的，上游 prompt cache 永远对不上。

**对照实验**（`opencode_affinity.py`，同一段 2 911 token 的 prompt 连发两次）：

| 第二发带什么 | round2 `cached` |
|---|---:|
| 什么都不带 | **0** |
| 固定 `x-session-affinity: <同一个值>` | **2 816 / 2 911 = 96.7%** |

**join 分析**（`join_hit.py`）：log 的 `request_id` ↔ recordings 的 `request_uuid`
**join 不上**，必须用 `(provider, tokens_in, tokens_out, timestamp ± 5 s)` join
（实测 matched 12 779 / 12 780）：

| provider | 有 session | 无 session |
|---|---:|---:|
| opencode_go_1 | 66.7% | 1.4% |
| opencode_go_2 | 63.5% | 0.0% |
| xiaomi | 94.7% | 33.2% |
| minimax | 97.4% | 29.8% |

两个被排除的怀疑：

- **body 里的 session id 永远是冗余的** —— `bodyonly% = 0.0%`，全 provider 一致；
  `upstream_request_filter` 在读下游 body 之前跑，body 里的 `prompt_cache_key` 本来就拿不到。
- **跳 provider 不是主因** —— 同 provider 连续 vs 跳 provider 的命中率 **65.97% vs 64.19%**，
  几乎没差；no-session 请求只占 **0.1% token**（opencode 111 reqs / 134k tok）。

### 14.4 修复①：opencode affinity 优先级重写 + 来源计数器

`upstream_request_filter` 改成四段优先级，**每段都计数**：

```
客户端自己的 x-opencode-session
  → x-session-affinity / x-opencode-session-id
  → ctx.session_id（request_filter 从 x-session-id / x-conversation-id / session-id / thread-id 解析）
  → 全新 UUID
```

只有 `source != "client"` 才写入 header；新指标
`maxllm_opencode_affinity_source_total{source=client|affinity_header|session|generated}`。

**双机实测**（部署后 41 min）：`session = 285`、`generated = 16` —— `generated` 全是没有
任何 session header 的探针类小请求（`tin = 37 / 90`）。

### 14.5 修复②（A）fast-cohort 收紧 + 封顶；（B）sticky pin 速度逃生，与 A 解耦

常量（`gateway.rs`）：

```rust
AUTO_POOL_COHORT_MEDIAN_MULTIPLIER = 1.5    // 原 2.5
AUTO_POOL_COHORT_FLOOR_MS          = 5_000.0
AUTO_POOL_COHORT_CEILING_MS        = 10_000.0    // 新增
// ceiling = min( max(median*1.5, 5000), max(10000, median) )  ← 永不低于 median

AUTO_POOL_PIN_ESCAPE_MULTIPLIER = 1.5
AUTO_POOL_PIN_ESCAPE_FLOOR_MS   = 10_000.0
fn auto_pool_pin_escapable(pinned_ms, median) -> bool {
    pinned_ms > median * 1.5 && pinned_ms > 10_000.0
}
```

**关键设计：A 和 B 解耦。** 轮转走 `auto_pool_fast_cohort`（受 A 收紧影响），
**pin 分支改用 `!pin_is_slow()`，不再复用 `in_fast_cohort`** —— 否则收紧 A 会顺手把
「只比中位数慢一点」的暖 pin 判死，每次抖动都要重付一遍 prompt cache，正好与本轮目标相反。

**测试**：

- `test_auto_pool_keeps_a_warm_pin_that_is_only_above_the_cohort`：6 s 的 pin（低于 10 s 逃生地板）
  必须**保留**，同时轮转必须跳过 slow（`maxllm_auto_pool_slow_skips_total{provider="slow"}` 非 0）；
- `test_auto_pool_escapes_a_pin_that_is_slow_enough`：`set_delay(10_500 ms)`，pin 后下一请求必须逃到 fast；
- 单测 `cohort_ceiling_is_banded_and_never_below_the_median`、
  `pin_escape_needs_both_relative_and_absolute_pain`。

`cargo fmt --check` / `cargo test -p maxllm-translate -p maxllm-gateway -p maxllm-admin`
（63 + 180 + 139 passed，4 ignored）/ `clippy -- -D warnings` 全绿。

### 14.6 修复③：`request_logs` 落 `cached_tokens` + `session_id`（带在线迁移）

- `models.rs` 的 `RequestLog` 加 `cached_tokens: u64`、`session_id: Option<String>`；
- `sqlite_store.rs` 的 `CREATE TABLE` 加列，并新增 `migrate_request_log_columns()`：
  `PRAGMA table_info(request_logs)` 发现缺列就 `ALTER TABLE`，`init_tables()` 末尾调用；
- INSERT 改 17 占位符 + row reader + 测试；`api.rs` 测试构造补字段；
- 新查询脚本 `cache_hit.py`（NAS `/tmp/cache_hit.py`、本机 `~/opt/maxllm/cache_hit.py`）：

```sh
python3 cache_hit.py --since 2026-11-08T18:24:00Z   # 或 --last-minutes N
```

只统计 `200 <= status < 400`。⚠️ **必须传 `--since` 部署时刻**
（`2026-11-08T18:24:00Z` = 02:24 CST）：窗口一旦跨过迁移点，旧行的 `cached_tokens` / `session_id`
全是 `0` / `NULL`，命中率会被拉到 3%。判别信号：`nosess% ≈ 100%` 就说明跨了迁移点。

### 14.7 看板

`scripts/openobserve_dashboard.py --apply` → dashboard `7525075825075421184`：
**21 → 22 panels**，新增 “Prompt cache hit rate by provider”（查询带 `or ... * 0` 零填充）。
复测阶段给该查询补了一道 `and ... > 10000` 活动量下限：30 分钟窗口里没有实质流量的
provider（空闲、或只有几十 token 的健康探针）本来就没有命中率可言，却会被 `or ... * 0`
渲染成 0% 红条；下限把它们从图上摘掉，0% 只留给「在服务但一发都没命中」的真冷 provider。

### 14.8 双机部署（2026-11-09 02:23 / 02:24 CST）

| 主机 | 备份 | 启动 | NRestarts |
|---|---|---|---|
| NAS | `maxllm-server.bak-20261109-022310-pcache` | 02:23:46 | 0 |
| 本机 | `maxllm-server.bak-20261109-022401-pcache` | 02:24:35 | 0 |

musl release 二进制；两台 `/health` `status=ok`、`version=0.3.10`；
NAS 迁移后 `PRAGMA table_info(request_logs)` 已有 `15 = cached_tokens`、`16 = session_id`。

### 14.9 复测（部署后窗口，`cache_hit.py --since <部署时刻>`）

窗口 `2026-11-08T18:24:00Z → 19:25:00Z`（部署后 61 min，NAS，`cache_hit.py --since`）：

| provider | reqs | tokens_in | 命中率 | 判定 |
|---|---:|---:|---:|---|
| opencode_go_2 | 523 | 74 783 959 | **98.67%** | ✅（修前 64.83%） |
| volc_1 | 182 | 29 740 131 | **97.57%** | ✅ |
| volc_t_1 | 22 | 2 668 052 | **97.10%** | ✅（修前同窗口 89.63%） |
| xiaomi | 73 | 11 659 527 | **96.98%** | ✅（修前 94.76%） |
| minimax | 126 | 16 761 756 | **93.18%** | ⚠️ 见下 |
| opencode_go_1 | 2 | 301 813 | 71.89% | 样本 2（乒乓消除后流量归到 go_2） |
| ollama_local | 4 | 148 | — | 只剩探针（`tin=37/90`） |
| **合计** | **932** | **135 915 386** | **97.52%** | |

**同长度前后对照**（各 60 min，日志口径，排除迁移影响）：

| provider | 部署前 60 min | 部署后 61 min | Δ |
|---|---:|---:|---:|
| opencode_go_2 | 59.89% | 98.67% | **+38.78pp** |
| opencode_go_1 | 56.61% | 71.89% (n=2) | +15.27pp |
| volc_t_1 | 89.63% | 97.10% | **+7.47pp** |
| volc_1 | 95.90% | 97.57% | +1.67pp |
| xiaomi | 96.89% | 96.93% | +0.04pp |
| minimax | 97.37% | 93.18% | −4.18pp |
| **合计** | **82.85%** | **97.51%** | **+14.67pp** |

**运行时指标**（部署后 61 min）：
`session_affinity_hits=910 / misses=6 / evictions=0`；
`opencode_affinity_source{session}=524 / {generated}=26`（`generated` 全是无 session 头的探针）；
`auto_pool_slow_skips{opencode_go_1}=15 / {xiaomi}=13 / {minimax}=5 / {opencode_go_2}=1`。

**关于 minimax 的 93.18%（唯一的主力红灯）** —— 不是回归，是分母变了：

- **绝对 miss 反而降了**：同长度窗口 1.405 M → 1.143 M token；
- **流量掉得更多**：53.4 M → 16.76 M token（−68%），而冷启动次数没跟着掉 ——
  该窗口 4 次近全 miss（196 k / 211 k / 177 k / 214 k，全部落在同一个
  `session=01a1b6f7` + `/v1/responses/auto` + `MiniMax-M3` 上）就是「同一个粗粒度
  session 底下新起的对话」首次落到已 pin 住的 minimax，属于不可避免的首触；
- **`hop = 0`**：按 `(session, path, model)` 分组后，minimax 窗口内没有任何
  「上一条在别的 provider」的请求 —— A/B 没有把对话踢来踢去；
- 9 小时基线里 minimax 是 **97.30%**，长期口径本来就过线。

同一张分解表在部署后窗口重算，全池 `hop` 都是 0 或个位数
（`volc_t_1` 58 k、`volc_1` 2 k），说明 sticky 在部署后是**真的钉住了**。

### 14.10 残余归因：miss 里网关能动的那份已经动完了

把基线窗口按 `(session, path, model)` 分组（比裸 session_id 可靠 —— 生产上一个 session id
底下挂着多段不同上下文的对话），对每组内**同 provider 连续**的请求把 miss 拆成四类：

- `first`：该组在该 provider 上的第一次请求（冷启动，不可避免）；
- `hop`：上一条同组请求落在别的 provider（**网关可动**）；
- `reset`：`miss > 50% × tokens_in`，前缀整体换了（客户端压缩上下文 / 换分支，网关动不了）；
- `incr`：其余（新 turn 的增量；实测 `cached` 恰好等于上一发的 `tokens_in`，
  说明上游**不把自己的输出写回前缀缓存**，这部分只能靠 provider 侧）。

| provider | 命中率 | first | hop | reset | incr |
|---|---:|---:|---:|---:|---:|
| minimax | 97.30% | 3.8% | 0.0% | 59.5% | 36.7% |
| xiaomi | 94.95% | 6.7% | 0.0% | 56.4% | 36.9% |
| ollama_local | 96.29% | 14.1% | 0.0% | 38.3% | 47.6% |
| volc_1 | 95.83% | 11.6% | 3.5% | 29.1% | 55.8% |
| volc_t_1 | 94.67% | 9.1% | 22.0% | 40.2% | 28.7% |
| opencode_go_1 | 65.59% | 1.8% | 26.9% | 62.5% | 8.8% |
| opencode_go_2 | 70.75% | 2.2% | 21.0% | 63.4% | 13.4% |

结论：**opencode 两个池成员的 `hop`（21–27%）就是 §14.3 的 affinity 根因**
（`opencode_go_1 ↔ go_2` 互相乒乓 437 次），修完 sticky 就不再产生；
`xiaomi` / `minimax` / `ollama_local` 的 `hop` 已经是 **0**，剩下的 `reset` + `incr`
是客户端上下文行为与上游缓存语义，**网关侧没有下一个同量级的杠杆**。
`volc_t_1` 的 `hop`（22%）来自 `volc_1 ↔ volc_t_1` 同模型（`deepseek-v4-flash`）的
pin 抖动，A/B 落地后 `session_affinity_misses_total` 只有个位数，窗口拉长会自然收敛。

### 14.11 两个红灯的真根因：慢 provider 被永久封禁（修复 C，`7e1b9db`）

A+B 落地后 `minimax`（93.18%）与 `opencode_go_1`（71.89%）**再无流量**，窗口冻结在
各自被撤离的那一刻（19:07:37Z / 18:24Z）。按 `(session, route_path)` 分组看清了过程：

| session | provider 迁移链 | 请求数 |
|---|---|---:|
| `01a1b6f6-cef2-` | go_1 → volc_1 → volc_t_1 → volc_1 → volc_t_1 | 175 |
| `01a1b6f7-225f-` | go_1 → **minimax** → volc_1 → volc_t_1 | 403（minimax 的 126 发全在此） |
| `01a1b6f7-4c39-` | volc_1 → **minimax** → volc_1 → xiaomi → volc_1 | 491 |

minimax 在 19:07:37Z 打出最后一发、耗时 **88 903 ms** 后整条 session 逃走，再没回来。
`maxllm_auto_pool_slow_skips_total` 同期累到 `go_1=39 / minimax=29 / xiaomi=31 / volc_1=4`。

**两个 bug 叠加（都在 `provider_stats.rs`）：**

1. **证据永不过期** —— `LatencyStats { ewma_ms, samples }` 没有时间戳。provider 被判慢
   → 轮转不再选它 → 没有流量 → 估计值永远不更新 → **判定成了永久封禁**，而它的
   上游 prompt cache 在旁边一起变冷。
2. **EWMA α=0.3 对离群值过敏** —— 实测（`ewma.py`，窗口内全部成功请求）：

   | provider | n | p50 | EWMA | 逃出 pin? | 在 cohort? |
   |---|---:|---:|---:|---|---|
   | ollama_local | 17 | 1 033 | 1 048 | 否 | 在 |
   | opencode_go_2 | 1 270 | 4 543 | 3 983 | 否 | 在 |
   | volc_t_1 | 273 | 3 889 | 4 262 | 否 | 在 |
   | volc_1 | 729 | **6 585** | **10 792** | 否 | **被误踢** |
   | minimax | 126 | **5 064** | **31 744** | **是** | 被踢 |
   | opencode_go_1 | 2 | 41 416 | 34 568 | 是 | 被踢 |
   | xiaomi | 165 | 25 357 | 74 202 | 是 | 被踢 |

   一次 88.9 s 的离群把 minimax 从 ~5 s 顶到 ~32 s；volc_1 的 p50 只有 6.6 s 却被
   10.8 s 的 EWMA 判出局。

3. **再测量机制实际失效** —— `AUTO_POOL_EXPLORE_EVERY=50` 按**轮转次数**计，而
   affinity 命中 2 511 : miss 11、2.75 h 只发生约 33 次轮转 ⇒ 探索轮转 0~1 次。
4. **冷启动反馈回路** —— 被封的 provider 一旦重新有流量，第一发必然是全前缀 prefill
   （慢）→ 又被判慢 → 又被封 + session 逃走 → 只留下一次 0% 冷 miss，数字更差。

**顺带收口：`tin=37/90` 探针来源** —— 是本机健康检查脚本的 `probe_infer`
（`POST /v1/responses/auto`，`input="reply with the single word: ok"`，≈90 token）与
`probe_resp`（`POST /v1/responses`，`deepseek-v4.1-flash:cloud`，≈37 token），由两个
每 10 分钟的 timer 拉起；指标导出脚本只 `GET /health`、`/metrics`，从不发请求。
量级 ~90 token/发，对命中率可忽略，未改。

**修复 C（commit `7e1b9db`）—— 三层，全在 `provider_stats.rs`：**

| 层 | 做法 | 效果 |
|---|---|---|
| 证据会过期 | `TOTAL_LATENCY` 改成 10 分钟滑窗（`TOTAL_LATENCY_WINDOW`），空窗读作 `None` | 静默 >10 min → 回到 **unmeasured** → 重新进入 fast cohort，封禁不再是终局 |
| 中位数而非 EWMA | 取窗口样本的中位数（偶数取靠上中间） | 单个 90 s 离群定不了 provider 的生死 |
| 冷样本降级 | `warm = cached_tokens_in × 2 ≥ tokens_in`（命中 ≥50% 才算 warm）；窗口内有 warm 就只用 warm 中位数；没有 warm 且样本 `< 5` → `None`；≥5 才退化取全冷中位数 | 不拿「全前缀 prefill」判 provider；同时保证从不报 cache hit 的 provider 仍可测、pin 逃得掉 |

**门禁**：`cargo fmt --check` / `cargo test`（admin 63 + gateway unit 185 + gateway
integration 139 passed / 4 ignored + translate 242 + doc-tests 1，**0 failed**）/
`cargo clippy --all-targets -- -D warnings` 全绿。
新增 5 个单测（中位数、单离群不动、证据过期、冷不压热、纯冷 5 发仍可测）+ 2 个集成测试
改用 `openai_response_warm`（否则 mock 不报 `cached_tokens` → 全判冷 → 测不到行为）；
再补 1 个回归测试 `cohort_readmits_a_provider_whose_evidence_aged_out`（`f274c7c`，
证据过期后重新进入 cohort，同时断言档位中位数变严、慢样本仍被排除）。

**CI**：push 不触发 Actions（仓库是手动 `workflow_dispatch`），已补跑
`gh workflow run CI --ref main` → run **`42528882208`**（2026-11-09 22:10Z 触发），
Format / Check / Clippy / Test / Responses policy / Ops scripts / Build Release **七项全绿**
（Docker Build & Push 按条件跳过）；仅剩官方 runner 的 Node.js 20 弃用与 ubuntu-latest
迁移预告两条 annotation，与本次改动无关。

**双机部署（2026-11-09 05:45 / 05:47 CST）**：musl static-pie release，
sha256 `787844f3f5205ea7ba9f7f92b654d078f9441d87040421251240d009d2efe738`；备份
`maxllm-server.bak-20261109-0545-slowverdict`（NAS）/ `-0547-`（本机）；
两台 `active`、`NRestarts=0`、`/health status=ok version=0.3.10`、journal 无 panic。

**诚实边界**：修后两家能重新被选中，但要过 95% 仍需**足够多的热而持续**流量
（minimax ≈10–15 M 热 token、go_1 ≈2.3–3.5 M，约 15–20 发长 session）。
一次性冷探针只会把数字拉低 —— **不为凑数人为喂流量**，等客户端自然回归。
`xiaomi` 走 `/v1/responses`（非 auto 路由），generic 候选路径不查 `pin_is_slow`，
本修复不影响它的钉住行为。

### 14.12 修复 C 复测（2026-11-09 06:35 CST，部署后 50 min）

**门禁 + CI**：本地 `fmt` / `test`（admin 63 + gateway unit 185 + gateway integration
139 passed / 4 ignored + translate 242 + doc-tests 1，0 failed）/ `clippy -D warnings`
全绿；CI 手动触发 run **`42528882208`** 七项全绿（Format / Check / Clippy / Test /
Responses policy / Ops scripts / Build Release），已补记进 §14.11。
回归测试 `cohort_readmits_a_provider_whose_evidence_aged_out`（`f274c7c`）钉住
「证据过期 → 重新进入 cohort」，且断言档位中位数掉下来之后**更慢的样本反而被排除**
（过滤器变严不是变松）。

**命中率**（NAS，`cache_hit.py --since 2026-11-08T21:45:00Z` = 修复 C 部署时刻）：

| provider | reqs | tokens_in | 命中率 | 判定 |
|---|---:|---:|---:|---|
| opencode_go_2 | 440 | 58.6 M | **97.50%** | ✅ |
| volc_t_1 | 192 | 28.8 M | **97.38%** | ✅ |
| volc_1 | 172 | 22.9 M | **97.02%** | ✅（22:03Z 起配额熔断，见下） |
| xiaomi | 134 | 20.4 M | **98.03%** | ✅ |
| ollama_local | 78 | 9.7 M | **95.53%** | ✅ |
| minimax | 1 | 183 | 69.95% | ⚠️ 冷首发，分母无意义 |
| **合计** | **1 017** | **140.3 M** | **97.34%** | |

30 分钟滚动窗口（22:05Z→22:35Z）：`go_2 98.08 / volc_t_1 97.27 / xiaomi 97.61 /
ollama_local 96.63`，合计 **97.55%** —— 有实质流量的 provider **全部 >95%**。

**修复 C 生效的直接证据**：

- `minimax` 在修复前最后一次出流量是 `19:07:37Z`（被永久封禁），修复 C 部署后
  **`22:25:04Z` 重新被选中并成功服务**（`MiniMax-M3`，1979 ms），封禁不再是终局；
- `auto_pool_slow_skips_total` 自重启起只累到 `xiaomi=6 / ollama_local=1`，
  **没有 `minimax` / `opencode_go_1`** —— 两家不再是"被 cohort 踢出去"的状态；
- `session_affinity_hits=1010 / misses=1 / evictions=0 / size=115`，按
  `(session, route_path, model)` 复核 `hop = 0`：sticky 没有乒乓，缓存没有被网关自己打散。

**两盏残余红灯的真实状态（不是回归，是分母还没长出来）**：

- `minimax` 只有 1 发 / 183 token，69.95% 是一次全前缀冷 prefill 的瞬时值；
  要过 95% 需要 §14.11 说的 ≈10–15 M 热 token；
- `opencode_go_1` 部署后 **0 流量**：它和 `go_2` 同池，session 全被 affinity 钉在
  `go_2` 上（1 010 : 1），只有**新 session** 才会轮转到它，43 min 只出现了 1 个。
  它的 71.89% 是 2 发 / 301 k token 的历史冻结值。
- **不人为喂流量**：冷 miss 只会把数字拉低，等客户端自然回归（§14.11 诚实边界）。

**同期两个非缓存观察**（记录，不改）：

- `volc_1` 在 `22:03:48Z` 后进入配额熔断（`circuit_breaker_state=1`）—— 与 §12.4
  同源的真配额耗尽，不是误熔断；30 分钟面板里它自然掉出（无流量），全窗口仍 97.02%。
- `ollama_local` 全窗 95.53% 的主要拖累是两条**全新上下文冷首发**
  （`tokens_in` 145 303 / 131 536、`cached=0`，合计 277 k miss ≈ 该家 miss 的 3/4），
  加上 4 发 37/38 token 的健康探针；30 分钟窗口内它是 96.63%。

**三节点一致性仍是唯一硬阻塞**：NAS / Fedora = `787844f3…2efe738`（含 A+B+C），
VPS 停在 `23056546…166461`（2026-11-08 21:40 部署），本会话无 ssh 凭据
（dotvault 无该机条目、无 `id_rsa`、root / oliveagle 均被拒）→ 待有凭据的会话跑
`ops/machines/vm_tecent_cloud_01/maxllm/deploy.sh`，状态已写进该节点 README 文首。

### 14.13 `/health` 两节点读数不一致的归因（2026-11-09 06:5x–07:0x CST）：不是 bug

**现象**：同 sha（`787844f3`）同 `maxllm.toml` 的两台，`/health` 读数差一倍 —— Fedora
`providers 31/32`、`auto 11/12`、`auto_home 14/14`；NAS `providers 17/32`、`auto 9/12`、
`auto_home 3/14`，`half_open` 12 家全是 home 系。四步判定链，全部实测：

1. **端点可达性两台一致 ⇒ 排除「NAS 网络不可达」**。14 个 http `base_url` 分别从两台
   curl，结果逐条相同：`.20:11050` 404（活）、`.10:18195` 404（活）、`.4:11434` 200（活）、
   `.10:11060` 活但 NAS 侧 4s 超时（慢而不死），其余 `.20:11051/52/53`、
   `.10:10001/11023/11025/11050/11053`、`.20:11022/11023` 全部拒绝/空回复 ——
   **14 个 home 端点里 11 个真死了**（GPU 服务没起，算力侧事实，不是网关问题）。
2. **NAS 读数与端点现实逐条对齐 ⇒ NAS 是诚实的**。`auto_home` 14 家中恰好 3 家 closed
   = `olefedora_gpu0_minicpmv47` + `coding_home_olefedora_gpu0`（都指活的 `.20:11050`）
   + `humanizer_gpu3`（`.10:11060`）；half-open 12 = 11 家死的 auto_home 成员 +
   `oleworkstation_rerank`（`.10:11025`，死）。breaker DB：这 12 家 `failures=3`、
   `updated_at=2026-11-08T11:39:40Z` —— 当时一串 `/v1/responses/auto_home` 真实请求
   （client `127.0.0.1`，11:39:41–57Z）经 failover 链把死候选各踩满 3 连败开闸、
   最后落在活端点上（request_logs 只记成功者，中间连败只进 breaker），fail window 早已走完
   → `effective_state()` 投影为 HALF_OPEN（下一次真实流量拿一次试探名额，成功即回 closed）。
3. **Fedora 是「无罪推定」⇒ 流量历史不同，不是状态机分叉**。它的 `circuit_breaker_state`
   只有 11 行、**home 系一行都没有**（重启后 request_logs 只有 `/v1/responses/auto` 与
   `/v1/responses`，从没碰过 home provider）→ state 停在初始 CLOSED → 读作 healthy。
   证据驱动熔断的固有语义：没证据不定罪，第一家真实流量失败会自己开闸自愈。
4. **探针不进熔断是刻意设计，不动**。NAS 上 `maxllm-pool-health.timer` 每 5 min 发带
   `X-MaxLLM-Probe: 1` 的最小请求（06:58 CST 一轮 `pool=26 healthy=10 failed=16`），
   `gateway.rs:4714/6228` 把探针排除在熔断计量之外（`pool_health.py` 注释同款声明），
   `SuccessExitStatus=0 1` 吞掉退出码。让探针推进熔断 = 一次探针抖动直接开关生产闸，
   违背设计注释，**不改**；探针对开闸 provider 拿 503 `circuit open (quota force-open)`
   进不了上游，也是同一条语义。

**历史 bug 已修、不在现场**：11-08 的「过滤器逐候选调 `is_open()` 吃掉 probe 槽 → 16/25
永久 half-open 503」（§14 附表 P1 行）已由纯读 `allows_traffic()` 修复（`79bb595`），
`git merge-base --is-ancestor 79bb595 HEAD` = 是 ⇒ 含在 `787844f3` 部署里；现场 12 家
half-open **全部**指向 curl 实测拒绝的端点，与该 bug 形态（活端点也 503）不同。

**附带澄清 kimi（保留未动）**：`force_open_until=2026-11-08T22:51:12Z` 已过 → /health 把它
数进 healthy，但 `quota-probe-kimi.jsonl` 07:01 CST 仍 429（7 天额度未恢复）→ 每个 cooldown
周期试一次、429、再开闸（`quota_opens=94` 的循环）—— `QuotaUnknown → 默认冷却` 的设计节奏，
不是状态机错；`maxllm.toml` 全程未改，kimi 在池。

**结论**：`/health` 语义（half-open 单列、`providers.healthy` 只数 closed）保持 —— 把
half-open 记为 healthy 会把 11 个真死端点洗成绿的；两台差异用「流量历史 + 探针不进熔断」
解释完毕，**无代码改动、无需重部署**。真正待办是 ① 11 个 home 端点本身（算力侧）
② 探针失败没有出口（`SuccessExitStatus=0 1` + webhook 仍是占位，§12.7 待办②）。

### 14.14 缓存命中率续测（2026-11-09 07:02 CST）

`cache_hit.py --since 2026-11-08T21:45:00Z`（修复 C 部署窗，1 919 请求 / 265.9 M input）：

| provider | reqs | hit% |
|---|---:|---:|
| opencode_go_2 | 1 052 | **97.87%** |
| volc_t_1 | 428 | **96.94%** |
| xiaomi | 182 | **97.58%** |
| volc_1 | 172 | **97.02%**（22:03Z 起配额熔断，08:07:51 CST 恢复待观察）|
| ollama_local | 83 | **95.53%** |
| minimax | 2 | 69.95%（冷首发，分母未长出）|
| **合计** | **1 919** | **97.46%** |

配额熔断三家的恢复时刻（`reset_at_ms` 由 `learned_pattern`/`body_regex` 从上游 429 body
解析，逐条复核，**不是误熔断**）：`volc_1` → **2026-11-09 08:07:51 CST**
（`1794182871000`；此前口传的 08:47 是换算笔误，已订正）、`zhipu_1` → 11-10 22:58:54 CST
（body_regex，与上游「2026-11-10 22:58:54 重置」逐字一致）、`volc_2` → **11-27 23:59:59 CST**
（上游明示 **monthly** 配额耗尽）—— 即 `auto 9/12` 缺的正是这三家，恢复观察已挂后台采样。

长窗（18:24Z 起，采样器每 10 min 写 `/tmp/cache_hit_series2.txt`，约 10:31 CST 收官）：
`go_2 98.35 / volc_t_1 97.42 / volc_1 97.17 / xiaomi 96.79 / ollama_local 95.52 /
minimax 93.18（128 发，从冷首发爬坡）/ go_1 71.89（2 发冻结）`，合计 **97.66%** ——
有实质流量的 provider **全部 >95%**；minimax / go_1 两盏灯等自然流量把分母长出来，
不人为喂（§14.11 诚实边界）。journal 自 05:45 部署起 `warning+` 0 条，
`request_logs` 0 条 status≥400（§14.12 后持续）。

### 14.15 watch 巡检两处 FAIL 的根因与修复（2026-11-09 07:18–08:01 CST）

`maxllm-watch.service`（Fedora 侧每 10 min 巡检 + 一级修复）07:18 / 07:40 两轮
`infer FAIL 无终止事件`、07:52 轮 `resp FAIL HTTP=000`，07:29 轮更是一次
`health,auth,infer,resp` 首轮全挂并触发 repair 误重启（graceful drain 35s）。
逐条查实为 **一假一真 + 一起未复现的连接层事件**：

**① infer「无终止事件」= 巡检脚本 bug（假阳性 ×4，已修）**。脚本取最后一条 `data:` 行
喂 jq 解析 `.type` / `usage`；而 volc 系上游在 `response.completed` 之后还发一行
`data: [DONE]`（不是 JSON）→ jq 失败 → `etype= / tokens=0 / text=''` → 落进
「无终止事件」FAIL 分支。证据闭环：

- 四轮假阳性（06:46 首轮 / 07:18 / 07:30 / 07:40）网关侧全部
  `request completed provider=volc_t_1 … truncated=false`（07:18 那笔
  `tokens_in=90 tokens_out=21 fallback=true` = open_design 402 后的重试链，上游完整回了
  21 token，网关没有少转发）；
- PASS 轮（06:24 / 06:36 MiniMax、07:07 opencode）的收尾都不带 `[DONE]`；
- 离线对拍：同一判分逻辑，volc 收尾体（末行 `data: [DONE]`）旧逻辑 `etype` 空 → FAIL，
  新逻辑取 `^data:\s*\{` 的 JSON 行 → `response.completed tokens=99` → PASS；
  MiniMax / opencode 两种收尾新旧逻辑均 PASS。

修复 = 只取 JSON 型 data 行（`rg '^data:[[:space:]]*\{'`），FAIL 分支补 `curl_rc=` 与
`bytes=`。**live 验证**：07:52 轮 infer 落在 volc 路由（`deepseek-v4-flash-ga-260731`，
收尾带 `[DONE]`）→ PASS `tokens=102`；08:01:34 定时轮再次落在同一路由 → PASS
`tokens=114`，七项全 OK。

**② resp `HTTP=000` = LAN Ollama box 的 ollama serve 卡死（真故障，已修）**。resp 探针
pin `deepseek-v4.1-flash:cloud` → 主路 `ollama_local`（`.4:11434`）。现场：

- 网关侧 `request completed provider=ollama_local … tokens=0 latency_ms=60000` 连续 8 笔
  （23:46:48Z 起；最后一笔成功 23:40:18Z）；
- 直连该 box：`GET /api/tags` 200 / 2.7 ms，但 `POST /v1/responses` 无论 deepseek 还是
  kimi 模型都挂满自身 60 s 超时后回 **502**（box journal 逐条 `502 | 60.0s`）；从 box
  直连 `https://ollama.com` 0.9 s / 200 ⇒ 网络与远端都好，是**进程内代理路径卡死**；
- 修法：`sudo systemctl restart ollama`（07:56:37 CST）→ 9.6 s 后同请求 200，
  07:57:04 巡检七项全 PASS。该 box 只有 2 个 cloud 代理模型 + 2 个 embedding
  （`ollama ps` 无驻留模型），重启零损失。

**③ 07:29 首轮四探针全挂（未复现，观测已补强）**：首轮 `health,auth,infer,resp` 全 FAIL
且首轮 3 s 内走完（resp 若挂会吃满 60 s ⇒ 排除 ② 的形态），repair 据此误重启了一次网关；
但同时刻网关在途请求照常完成（23:29:53.661Z 一笔 ollama 807 ms 正常完成）⇒ 疑似客户端到
网关端口的瞬时连接层抖动，细节已无法回溯。补强：四个 HTTP 探针全部加 `curl_rc=`
（区分 refused / reset / timeout），并新增 `first_checks` 首轮明细快照字段（repair 复检会
覆盖探针数组，此前只留 `first_overall/first_failed` 名字、细节全丢）—— 07:57 / 08:01 两轮
已带上该字段，下次复现即可定性。

巡检脚本：`ops/machines/oleFedora01/systemd/scripts/maxllm_health_check.sh`（ops 仓内
untracked，本仓不涉及）。08:01:34 定时轮 **七项全 OK**，连续失败计数归零。

### 14.16 volc_1 配额恢复确认 + 缓存续测（2026-11-09 08:02–08:09 CST）

- **volc_1 恢复实锤**：force window 08:07:51 CST（§14.14 订正值）到期后，后台观察器
  08:09:19 写 `RECOVERED health_open=['open_design','volc_2','zhipu_1']` 并退出 ——
  `/health` 不再列 volc_1（DB 里 `state=1` 是 breaker 原始枚举，effective 投影已回
  closed）。恢复后首笔 volc_1 流量等自然请求落池：`request_logs` 最后一笔仍是熔断前
  22:03:48Z。
- **缓存长窗**（02:24–08:01 CST，6 523 请求 / 947.8 M input，合计 **97.60%**）：
  `go_2 98.17 / volc_t_1 97.29 / volc_1 97.17 / xiaomi 96.70 / ollama_local 95.52` ——
  **有实质流量的五家全部 >95%**；minimax 132 发 93.18%（冷首发爬坡）、go_1 4 发
  71.87%（分母未长出，窗口内新增 2 发为 0%）仍是仅有的两盏灯，按 §14.11 纪律不喂流量。
- **watch 归零**：08:01:34 定时轮七项全 OK —— §14.15 修复后的首个定时轮，infer 恰好
  落在带 `[DONE]` 收尾的 volc 路由上也 PASS。

### 14.17 审计 #4 / #5 / #10 收口复核 + 探针排除实测（2026-11-09 08:2x CST）

**#5 provider 改名身份断裂 —— 三条实测全绿**

- `/metrics` 全文 `one_api` **0 行**（改名前 9 条僵尸 series）。
- breaker DB `circuit_breaker_state` 中 `provider like '%one_api%'` **0 行**，
  全表 30 行 = 配置 provider 数（改名前是 38 条 series vs 30 个 provider）。
- 旧 base_url 不再被前缀匹配静默吞掉，而是显式 404 点名：
  `POST /v1/responses/one_api_volc_1` → **404**
  `unknown provider or pool 'one_api_volc_1' in /v1/responses/one_api_volc_1; use
  /v1/responses/<provider>, /v1/responses/auto or /v1/responses/auto_home`
  （`one_api_zhipu_1` 同样 404）。修复前它是落到通用路由、被**别的** provider 作答
  且 `route_path` 记成 `/v1/responses` —— 与 §12.1 同族的"只校验不钉住"。

**#10 自检探针不再污染生产信号**

- 代码语义（`crates/maxllm-gateway/src/ctx.rs:233`）：带 `X-MaxLLM-Probe: 1` 的请求
  "served normally but excluded from metrics, request logs and circuit-breaker accounting"；
  `gateway.rs:2854` 读头、`:5813` 打 `health probe completed (excluded …)` 日志。
- 改动：巡检脚本 `probe_infer` / `probe_resp` 的 curl 各加 `-H 'X-MaxLLM-Probe: 1'`
  （health 探针打的是 unrouted 路径、本就不落账，未加）。
- 实测（08:21:08 手动轮 exit=0、七项全 PASS）：`request_logs` 在
  00:20:40–00:21:40Z 窗口内 13 行的 `tokens_in` 全部落在 68 317–235 913 的真实流量区间，
  推断探针的两笔（infer ≈90 tok、resp ≈38 tok）**一行都没有** ⇒ 排除生效。
- 旁证：`request_logs` 自 `2026-11-08T20:00Z` 起 `status >= 400` **0 条**
  （11-08 白天那批 loopback 4xx 属手工测试，不复现）。

**#4 `stream-name` 核验（原"本轮未核"项）**

- NAS `otel-collector-maxllm` 服务 **active**；live
  `~/opt/otel-collector-maxllm/config.yaml` 与 ops 仓
  `ops/machines/oleNas01/otel-collector-maxllm/config.yaml` **md5 一致**
  （`76274d36aaafba6daca69a399db4baba`）。
- 导出器已按信号分流出独立 stream：`stream-name: maxllm_recordings`（config:101）、
  `stream-name: maxllm_logs`（config:120）；config 注释明确 OTLP **metrics** 不吃这个头
  （OpenObserve 按 metric name 一 stream，避免留下空的 `maxllm_metrics`）。
- OpenObserve 实测（`/api/default/streams?type=logs` + `_search` 计数，08:28 CST 快照）：
  `maxllm_logs` **37 821** docs、`maxllm_recordings` **112 752** docs，
  最新 `_timestamp` = 2026-11-09 **00:28:37Z / 00:28:38Z**（= 08:28 CST，正在流入），
  两流首条均为 2026-11-08 06:08:30Z。
- **#4 残余只剩 1 项**：告警出口仍是 httpbin 占位（§12.7 待办②，需用户 webhook URL）。

---

## 15. 容量自消化：网关拥有错误 + 配额墙跨节点共享（2026-11-09 上午–下午）

### 15.1 事故：客户端看到的是上游的配额原文

11:15:04 CST，客户端（codex，base_url `http://10.126.126.199:18081/v1/responses/auto`）拿到：

```
rate limit exceeded: 您已达到每周/每月使用上限，您的限额将在 2026-11-10 22:58:54 重置。[20261109111504163e4b0369fd4cc0]
```

三个事实叠在一起，才让这句话出现在客户端面前：

1. **这是智谱的原文**，不是 maxllm 的话。末尾那串 `20261109111504163e4b0369fd4cc0` 是智谱的 x-log-id
   （格式 `YYYYMMDDHHMMSS` + 16 位 hex），网关把上游的 body 原样透传了。
2. **智谱的周配额墙是真的**：NAS 于 11-08 22:06 CST 学到 `zhipu_1` 的配额耗尽，
   `force_open_until=1794322734000` = **2026-11-10 22:58:54 CST**，与报错里的重置时刻一字不差。
3. **那一轮没有轮换成功**：请求体约 600–700 KB（194k input tokens），超过 pingora 的
   64 KiB 重放缓冲，老版本放弃 failover，把上游 429 直接交给客户端；而且当时
   **客户端连的是 VPS 节点**，它的熔断状态是自己的一份（11-08 21:40 刚部署、DB 为空），
   根本不知道另外两台已经把智谱判死刑了。

所以「不是要 auto rotate 么」——rotate 一直在，但那一轮既没有可轮换的候选
（智谱是这个模型唯一的 provider），也没有可重放的请求体。

### 15.2 现在的行为：网关拥有自己的错误

老网关把上游的状态码和 body 交给客户端，于是「上游配额墙」被客户端读成「我被限流了」，
agent 就停在那里等重置。新网关自己回答（`ctx::GatewayError`）：

| 上游状态 | 现在的回答 |
|---|---|
| 401 / 402 / 403 / 408 / 409 / 425 / 429 / 5xx | **503** `{"error":{"message":…,"type":"gateway_error","code":503}}` + `Retry-After` + `X-MaxLLM-Upstream-Status: <上游状态>` |
| 400 / 404 / 405 / 410 / 422 | 保留上游原样（这是客户端自己的错，`model not found` 这类诊断必须送达） |

上游的 body 一律不进客户端的响应体；上游状态码留在 `X-MaxLLM-Upstream-Status` 里供审计。

实测（本地节点，2026-11-09 12:46 CST）：

```
POST /v1/responses/kimi  → 503
{"error":{"code":503,"message":"no provider available: kimi=circuit open (quota force-open) — every candidate of route '/v1/responses/kimi' is rejecting traffic","type":"gateway_error"}}
```

### 15.3 轮换：预算、候选、请求体

* **failover 预算 ≥ 池子大小**（`failover_budget(hot, ctx)`）：老版本按路由配置的
  `max_failover_attempts` 走，池子有 12 个成员时预算可能比候选少，白白浪费成员。
* **候选**：被配额墙占住的 provider 立刻离开轮换，不再每轮拿它试一次。
* **请求体**：700 KB 的 body 在 native 与 translated 两条路径上都能重放并完成轮换
  （回归测试 `test_quota_429_fails_over_with_a_large_body`、
  `test_quota_429_fails_over_with_a_large_body_on_a_translated_provider`）。

实测（本地节点）：`POST /v1/responses` → 200，响应头 `X-MaxLLM-Provider: volc_1`、
`X-MaxLLM-Fallback-From: ollama_local` —— 主选失败，自动换人，客户端拿到正常答案。

### 15.4 配额墙怎么记、怎么跨节点共享

* **墙的长度已知**（上游给了重置时刻）：直接 park 到那一刻，`quota_cooldown_level` 归零。
* **墙的长度未知**（如 kimi 的 "weekly (7-day) usage limit"）：走阶梯
  `5m → 15m → 1h → 6h → 24h`（`QUOTA_COOLDOWN_LADDER_MS`），墙还在就升一级，
  探针成功则复位。阶梯的意义：不再每 5 分钟用一个请求去撞一次已知的墙。
* **跨节点共享**（`breaker_sync.rs`，新）：每个节点每 15 s 拉一次 peers 的
  `GET /admin/breakers`，**只延长**、不缩短别人的墙。学到墙的节点不再让每个节点各交一次学费。
* **运维预置**（`[quota_parking]`）：已知墙直接写进配置，所有节点启动即生效：

  ```toml
  [quota_parking]
  "zhipu_1" = "2026-11-10T22:58:54+08:00"
  ```

* **释放也是集群动作**（2026-11-09 13:0x 补）：运维在一个节点释放 provider，
  释放会带着它释放的那个截止时刻转发给 peers；peer 只有在**自己持有的截止时刻完全相同**
  时才丢掉这个 park（知道更晚的墙的 peer 保留自己的）。

### 15.5 节点拓扑（2026-11-09 13:0x CST 实测）

| 节点 | 地址 | 网关 | 熔断共享 |
|---|---|---|---|
| oleFedora01（本机） | 10.126.126.20 / 192.168.2.137 / 192.168.2.198 : 3795 | 0.3.12 / `b127ad9921b4b9d2` | ✅ 与 NAS 双向 |
| oleNas01 | 192.168.2.190 / 10.126.126.6 : 3795 | 0.3.12 / `b127ad9921b4b9d2` | ✅ 与本机双向 |
| 腾讯云 VPS | 10.126.126.199 : 3795（nginx : 18081） | **旧版本**（无 `/admin/breakers`） | ❌ 孤立 |

实测证据：

* 本机 park `cmd` → NAS 与 VPS 视图都在一个 poll（15 s）内变成 `open`，截止时刻逐毫秒相同；
* 本机 release `cmd` → NAS 立刻回到 `closed`（新版本的释放转发）；
* VPS 的 `10.126.126.199:3795/admin/breakers` 返回 404 —— **VPS 上的网关还是旧二进制**，
  它不参加熔断共享，也不认新的 breaker 端点；`18081` 上 `GET /admin/breakers`
  其实是 nginx 转到本机的（返回的就是本机那份状态），所以**别把 18081 当成 VPS 的状态源**。

### 15.6 待办：VPS 节点升级（需运维在本机执行）

```bash
# 1) 上传（静态 musl，sha256[:16]=b127ad9921b4b9d2）
scp ~/.cache/mx-gate-target/x86_64-unknown-linux-musl/release/maxllm-server \
    root@10.126.126.199:/opt/maxllm/bin/maxllm-server.new
# 2) 切换 + 重启
ssh root@10.126.126.199 'cd /opt/maxllm/bin && chmod 755 maxllm-server.new \
  && cp -p maxllm-server maxllm-server.bak-$(date +%Y%m%d-%H%M)-deploy \
  && mv maxllm-server.new maxllm-server && systemctl restart maxllm'
```

VPS 的 `maxllm.toml` 需要补上：

```toml
[breaker_sync]
enabled = true
interval_secs = 15
peers = ["http://10.126.126.20:3795", "http://192.168.2.190:3795"]
auth_token = "${MAXLLM_MASTER_KEY}"

[quota_parking]
"zhipu_1" = "2026-11-10T22:58:54+08:00"
```

若 VPS 的 ssh 别名 `vm_tecent_cloud_01` 可用，直接 `scripts/deploy_maxllm.sh vps`
（自带 static-pie 守卫、备份、20 s 健康检查与自动回滚）。

升级之前，**指向 VPS 的客户端仍会拿到上游原文 429**（老网关的行为）——
临时办法是把客户端的 base_url 指向本机或 NAS（两者都是新版本）。

### 15.7 轮换跟着缓存走（`03cfe3a` + `61c1483`，2026-11-09 13:19 / 13:5x CST 部署）

**问题**：轮换只看得见「快慢」，看不见「这一轮要不要把整段对话重读一遍」。
NAS 上 `/v1/responses/auto` 的 24 h 数据（25161 发）里，两件事混在一起：

| provider | 请求数 | 占比 | 缓存命中 | 平均耗时 |
|---|---|---|---|---|
| minimax | 8688 | 29.9% | 11.1% | 6.1 s |
| opencode_go_2 | 5337 | 18.4% | 85.7% | 8.4 s |
| volc_1 | 4194 | 14.5% | 67.4% | 9.5 s |
| ollama_local | 2490 | 8.6% | 3.0% | 16.1 s |
| volc_t_1 | 2333 | 8.0% | 88.0% | 10.4 s |
| xiaomi | 1702 | 5.9% | 1.3% | 34.1 s |

缓存没命中的那一轮 = 整段上下文重新 prefill = 一次全量计费 + 一次全量等待。
池子把 30% 的流量压在「几乎不缓存」的 minimax 上，又把 5.9% 压在
「既不缓存又最慢」的 xiaomi 上。

**做法**（`crates/maxllm-gateway/src/provider_stats.rs` + `gateway.rs`）：

1. **先量**：按 provider 记「这一轮有多少输入是从上游 prompt cache 里出来的」
   （EWMA，α=0.3，窗口 10 min）。输入 < 1024 token 的请求不计入 —— 20 token
   的请求要么命中要么不命中，说明不了长对话的事。
2. **轮换按缓存加权**：保对话热的成员拿大部分份额，重读整段的成员只拿保底 1 份。
   *没人被饿死*（池子得继续量它有的成员），没量过的成员拿已量成员的平均权重
   （轮换就是量它的那个东西）。份额是**交错**的，不是分组的 —— 分组会让连着
   几个请求全砸给同一个成员，那就不是轮换了。
3. **会话可以从冷成员上走开**：钉在「几乎不缓存」的成员上的会话没什么可失去的
   （下一轮本来就要重读），所以允许它迁到保对话热的成员上。门槛刻意收得很紧：
   钉住的成员至少服务过 3 轮（一次冷轮不算判决）、缓存比 < 25%、池子里确实有
   明显热的成员（≥ 50%）；没有热成员时**钉子不动**。迁移直接落到热成员上，
   不回轮换 —— 回轮换可能又把同一个冷成员发给它，那就叫「逃了但没逃掉」。

**新指标**：`maxllm_provider_cache_ratio{provider}`（百分比）、
`maxllm_session_affinity_cold_escapes_total{provider}`。

**实测（NAS，部署前后）**：

* 部署前 04:49–05:19 UTC：volc_1 257 发 / 53%、minimax 164 发 / 34%、
  opencode_go_1 34 发 / 7%、xiaomi 28 发 / 5.8%（xiaomi 单发 22.6 s）。
* 部署后 05:19 之后：volc_1 142 发 / 59%、opencode_go_1 54 发 / 23%、
  minimax 43 发 / 18%，**xiaomi 0 发**（它既慢又冷，权重把它挤出去了）。
* 最近 10 min：4565 发、缓存命中 97.3%、平均 9.5 s（24 h 全量口径 43.2% / 10.4 s，
  是被更早那段冷期拉下来的）。
* 指标本身在 NAS 上取到了真值：`volc_1 95%`、`minimax 97%`、`opencode_go_1 71%`。

**边界**：会话迁移会丢掉上游的 prompt cache —— 对冷成员来说没有 cache 可丢，
所以这条规则只在「几乎不缓存」时生效（< 25%）；缓存到 25%–50% 的成员保留会话。
真正的线程内容在请求体里，客户端每轮重发，迁移不丢内容。

**回归**：`test_auto_pool_prefers_the_member_that_keeps_the_conversation_warm`、
`test_a_session_pinned_to_a_cold_member_moves_to_a_warm_one`（集成），
外加 7 个单测（权重、交错、未测量成员、迁移落点、门槛、证据过期）。

### 15.8 缓存口径的补丁：新账号的第一轮不是判决（`b0f7e85` → 0.3.12，2026-11-09 13:56 CST 部署）

§15.7 的量法有个洞：**它把每一轮请求都算进了缓存比**。但一轮对话的**第一轮
天然是冷的** —— 上游从没见过这个前缀，它不可能命中。于是：

* 新加进池子的账号，第一轮就被判成「不缓存」；
* 权重把它压到保底 1 份；
* 它的会话因此被迁走 → 它再也没机会拿到能填缓存的会话；
* 它的样本永远攒不够 → 永远翻不了身。

**自我强化的降级螺旋**，而且恰好打在「我要往池子里加更多账号和 provider」这个
计划上。修法三条：

1. `RequestCtx::session_continuation`：只有**会话被钉住且钉住被兑现**的那一轮
   才算 continuation（`ctx.rs`）。`switch_to_fallback` 会把它清掉 —— failover
   目标从没见过这个前缀，它关于缓存的回答不算证据。
2. `record_cache_ratio` **只在 continuation 上调用**；输入 < 1024 token 仍然不计入。
3. **少于 3 个样本不是判决**：`auto_pool_cache_weight` 返回 `None`（「未测量」
   = 平均权重），`auto_pool_warm_target` 也不许把会话迁到一个还judge不了的成员上。
   没量过的成员要先拿到会话，才谈得上被评价。

两个缓存回归测试因此改写成**对话**（12 会话 × 3 轮、16 会话 × 4 轮），并且路由
必须显式 `session_sticky = true` —— 没有钉子就没有 continuation，没有 continuation
就没有证据。（原来那版没开钉子，所以一条缓存样本都没采到，池子永远均匀轮换，
测试看到的是 18/18。）

另外把 `test_least_connections_skips_the_busy_provider` 的 80 ms 固定 sleep 换成
「等 in-flight gauge 真的变成 1」——固定 sleep 是在跟测试机抢 CPU，整套跑的时候
它会红。

**部署**：NAS → oleFedora01，**0.3.12 / sha256[:16]=`b127ad9921b4b9d2`**
（musl static-pie，`scripts/deploy_maxllm.sh` 自带守卫/备份/健康检查/自动回滚）。
两台 `/health` 200、NRestarts=0，breaker 视图一致（zhipu_1、volc_2 两道墙共享）。

**顺带修掉的运维事故（回答「oleNas01 的 maxllm 挂了？」）**：今天 09:41:54 起，
NAS 的 `maxllm.service` 进了 **GLIBC_2.38 崩溃循环**（一个 glibc 构建被误拷上
NAS，systemd 每 5 秒重启一次，到 11:13:51 才被 musl 构建替换掉，restart counter
从 1 数到 1031）。业务流量当时由一个手工拉起的进程顶着，所以客户端没全断，
但**systemd 已经管不住它** —— 那 92 分钟里机器一旦重启，节点就是彻底空的。
`scripts/deploy_maxllm.sh` 的 static-pie 守卫就是为这次事故写的。

**另一处状态回退（本轮发现并修复）**：13:33 有一次部署把本机节点换成了
**billing-tier 分支的旧构建**（0.3.10，`strings` 里没有 `maxllm_provider_cache_ratio` /
`breaker_sync` / `quota_parking`，`/admin/breakers` 直接 `{"error":"not found"}`）——
备机没有熔断共享、没有配额停放、没有缓存加权。本轮已随 0.3.12 一起覆盖回
`b127ad9921b4b9d2`。**部署脚本必须从 `/tmp/mx-capacity` 这棵工作树跑**，
不要从其它分支的工作树跑。

**拓扑更正（§15.5 的补充，2026-11-09 14:0x 实测）**：入口
`10.126.126.199:18081`（VPS 上的 nginx）的 upstream 顺序是
**primary = VPS 自己的 :3795**，backup = oleNas01 → oleFedora01
（`~/.agents/ops/machines/vm_tecent_cloud_01/maxllm/nginx-maxllm-ha.conf`）。
三个实测把这件事钉死：

1. 带唯一标记的 `/v1/responses/auto` 请求打 :18081，**NAS 与本机的 recordings
   里都没有这条**（只有 VPS 的老网关在服务它）—— 之前「数据面已转给 NAS」的说法
   是错的，那只是把 NAS 的 DB 行误认成了自己的探针。
2. 以 assistant 轮结尾的 ark 请求打 :18081 → **400 `MissingParameter partial`**
   （VPS 的老二进制没带 ark 修复），同样的 body 打 NAS → **200**。
3. `POST :18081/v1/responses/zhipu_1` → 503 `no provider available: zhipu_1=circuit
   open (quota force-open)` —— VPS 的 0.3.10 已经会自己拥有错误（不再把上游原文
   429 交给客户端），但它**不认 `[quota_parking]`、不参加熔断共享**
   （`:3795/admin/breakers` → 404）。

**所以入口的 primary 仍是那台没升级的节点**，两个后果：
(a) ark 的 `partial` 400 落到客户端 —— nginx 默认的 `proxy_next_upstream` 不含
400，备份节点不会被试；(b) VPS 的熔断状态是自己的那份，它要自己撞一次墙才知道
（11:15 那次就是这样撞出来的），而另外两台 11-08 就已经知道了。

**唯一还差的动作就是升级 VPS 自己的那个网关**（需要 root 凭据，本会话没有）：

```bash
REMOTE=root@10.126.126.199 SOURCE=oliveagle@10.126.126.6 \
  ~/.agents/ops/machines/vm_tecent_cloud_01/maxllm/deploy.sh
```

在此之前，客户端若能直连，**把 base_url 指到 NAS（10.126.126.6:3795）或本机
（10.126.126.20:3795）** 比挂在 :18081 上更稳。

**上线核验（NAS，0.3.12 部署后）**：`maxllm_provider_cache_ratio` 现在**真的按续轮
量到了值**（minimax 98、ollama_local 97、opencode_go_1 99、opencode_go_2 98、
volc_t_1 100、xiaomi 98、volc_1 43），`maxllm_session_affinity_hits_total 85` /
`misses 8` / `size 117`（钉子真的在生效），`maxllm_session_affinity_cold_escapes_total
{volc_1}=1`（volc_1 只有 43%，有一个会话从它身上迁走了）。最近 10 分钟 161 发、
缓存命中 91.3%、平均 9.7 s。

注意：**NAS 的 `/metrics` 是 loopback-only**（远程访问回 403
`metrics is loopback-only; set [metrics] allow_remote = true`），要在 NAS 上
`ssh … 'curl -s 127.0.0.1:3795/metrics'` 才看得到；本机节点没有这个限制。

### 15.9 「不是要 auto rotate 么」：11:15 那次为什么没转（2026-11-09 14:0x–14:3x CST）

三件事叠在一起，缺一件都不会让智谱的原文落到客户端面前。

**第一层：那一轮没有可轮换的候选。** 智谱是那个模型在池子里**唯一**能服务的
provider（`/v1/responses/zhipu_1` 这条路由连 `fallback` 都是空的），所以
"rotate"当时是**有机制、没目标**。

**第二层：请求落在入口 primary —— VPS 上那台还没升级的 0.3.10。** 三条实测：

- `GET http://10.126.126.199:18081/health` → `"version":"0.3.10"`（NAS 与本机都是 0.3.12）；
- NAS 与 oleFedora01 的 `request_logs` 在 `2026-11-09T03:15Z`（= 11:15 CST）
  **一条 zhipu 记录都没有** —— 那次请求既没落在 NAS，也没落在本机，只可能在 VPS 上；
- VPS 的 `/admin/breakers` 返回 404（老二进制没有这个端点）—— 它**不认
  `[quota_parking]`、不参加跨节点熔断共享**，所以 NAS 在 11-08 22:06 就学到的
  智谱周墙（`force_open_until = 2026-11-10T22:58:54+08:00`，与报错里的重置时刻
  一字不差）它并不知道。11:15:04 它照发请求，于是自己撞上智谱的周墙。

**第三层：0.3.10 不"拥有自己的错误"。** `own_the_error`（401/402/403/408/409/425/
429/5xx → 503 + `Retry-After` + `X-MaxLLM-Upstream-Status`，上游 body 永不下发）是
`64540cd` 引入的，**在 v0.3.10 之后**才有。所以老网关把上游 429 的 body 原样交给
客户端，客户端读到的就是"我被限流了，等 2026-11-10 22:58:54"，而不是"这个 provider
的配额墙了，换一个"。已升级的两台现在会这么回答（NAS 实测）：

```
POST http://10.126.126.6:3795/v1/responses/zhipu_1  →  503
{"error":{"code":503,"message":"no provider available: zhipu_1=circuit open (quota force-open) …","type":"gateway_error"}}
```

**轮换本身是活的**（NAS 0.3.12 实测计数器）：
`maxllm_fallbacks_total{from_provider="kimi",to_provider="volc_1"}=1`、
`{from_provider="open_design",to_provider="volc_1"}=1`、
`maxllm_http_failovers_total{from_provider="kimi",status="429",to_provider="volc_1"}=1`。
大 body 也不会卡死：270 KB 的请求打 `/v1/responses/auto_home`（池里多数成员已下线）
返回 200，说明**整个请求体被完整重放**（`067320a`，v0.3.10 就已包含）。

#### 15.9.1 入口 nginx：主节点 5xx 时不会切备份（已修，待 VPS root 部署）

`ops/machines/vm_tecent_cloud_01/maxllm/nginx-maxllm-ha.conf` 里**根本没有
`proxy_next_upstream`** —— nginx 的默认只有 `error timeout`，**502/503/504 不会
切到备份**。于是老节点回 503（配额墙/池空）时，即使 NAS 好好的，客户端照样拿到
503。补丁（ops 仓库 `bdac746d`）：

```nginx
proxy_next_upstream error timeout http_502 http_503 http_504 non_idempotent;
proxy_next_upstream_tries 3;
```

`non_idempotent` 不能省：LLM 全是 POST，nginx 默认**不会**把已经发到上游的
非幂等请求再发给下一个上游 —— 没有它，这条规则对 LLM 流量等于没写。

生效需要 VPS root（本会话没有凭据）：

```bash
REMOTE=root@10.126.126.199 \
  ~/.agents/ops/machines/vm_tecent_cloud_01/maxllm/deploy-nginx.sh
```

在此之前，客户端 base_url 直指 NAS `http://10.126.126.6:3795/v1` 或本机
`http://10.126.126.20:3795/v1` 比挂 :18081 稳（用户的 4 台客户端
192.168.2.165/.106/.83/.198 本来就直连 NAS）。

#### 15.9.2 探针不再吃订阅配额（`c2f4d0d`）

`maxllm-pool-health.timer` 每 5 分钟给每个池成员发一次真实请求 —— 这是真实请求、
真实订阅额度：2026-11-09 它占了 NAS **全部上游请求的 21%**（约 250 发/小时），
其中 270 发打向**早就知道是墙**的成员（kimi 429 ×152、open_design 402 ×118）。
而探针流量被网关排除在 `request_logs` 与 `/metrics` 之外，**这笔浪费一直不可见**。

现在只问"还值得问"的问题（NAS 实测一轮）：

| 成员状态 | 判定 | 依据 | 探不探 |
|---|---|---|---|
| 网关已停放（熔断开着且有未来截止时刻） | `WALL` | `GET /admin/breakers` | 不探（volc_2 / zhipu_1 / kimi） |
| 最近真的服务过流量 | `SERVED` | **网关自己的账本** `request_logs` | 不探（8 个成员） |
| 上次探针吃到配额错误 | `QUOTA` | 探针自己的退避（每轮翻倍） | 暂时不探（open_design） |

"服务过流量"必须读**账本**而不是指标计数器：计数器在网关重启后从 0 开始，
会让每个成员都看起来"从没被用过"，退避就会失灵。账本（`request_logs`）是网关
自己的记录，重启也在。

实测：订阅成员的探针从 **12 发/轮降到 0**（只剩本地 GPU 盒子与 1 个 metered
成员被探），一轮省 10–12 发上游请求。

### 15.10 重启不再让池子失忆：路由证据落盘（`b47b42a`，2026-11-09 14:39 CST 部署）

**问题（实测，不是推演）。** auto 池的两个判决 —— 「这个 provider 的输入有多少
来自上游缓存」和「一次完整往返要多久」—— 只存在于进程内存
（`provider_stats::{CACHE_RATIO, TOTAL_LATENCY}`）。网关一重启，池子就**失忆重学**：
它不知道谁暖，于是会话被打散到别的 provider，上游的 prompt cache 随之冷掉。
NAS 实测（2026-11-09 05:59:49Z 重启）：

| 分钟（UTC） | 缓存命中率 | 用到的 provider 数 |
|---|---|---|
| 05:59（重启前） | 99.0 % | 3 |
| **06:00（重启后）** | **76.7 %** | **7**（ollama_local / xiaomi / volc_t_1 全冒出来） |
| 06:01 | 88.1 % | 4 |
| 06:04 | 98.9 % | 5（≈5 分钟才重新学回来） |

也就是说：**每次重启都要按「每个会话一次缓存未命中」的价钱付一次学费**，而重启
本身是为了可用性付的。熔断状态早就落盘了（`circuit_breaker_state`，配额墙因此能
跨重启存活），路由证据却没有 —— 同一类问题的两半，只修了一半。

**修法。** 把证据按「证据的年龄」而不是「证据的内容」跨进程搬：

- `maxllm-admin` 新增 `provider_stats_state` 表
  （`provider / cache_ratio / cache_samples / cache_age_secs / total_latency(JSON) /
  updated_at`），镜像 `circuit_breaker_state` 的写法；
- 网关每 **30 s** 把当前证据快照写进去（独立线程，SQLite 是阻塞的，不能占请求线程）；
- 启动时**带年龄**恢复：窗口（10 分钟）内过期的证据**不算证据** —— provider 回到
  *未测量*（unmeasured，轮换会接纳它），而不是带着一个已经过期的判决继续被排除；
  恢复出来的比率会立刻重新发布到 `maxllm_provider_cache_ratio`，**第一个请求还没来，
  面板上就已经是恢复后的数字**；
- 改名时证据跟着改名走（与熔断行一致），provider 从配置里消失则行被剪掉。

**验证（NAS 现场，两次重启对照）。**

| | 旧构建 05:59:49Z 重启 | 新构建 06:41:50Z 重启 |
|---|---|---|
| 重启后第 1 分钟缓存命中率 | 76.7 % | **98.2 %** |
| 重启后第 1 分钟用到的 provider 数 | 7 | **3**（与重启前一致） |
| 重启日志 | — | `Restored the provider routing evidence count=4` |
| 重启后 2 s（还没服务任何请求）的 `/metrics` | 空 | `opencode_go_1=99 opencode_go_2=97 volc_1=95 volc_t_1=99` |

部署：NAS `10.126.126.6` 与 oleFedora01 `10.126.126.20`，sha `1781604afb36aef5`
（`b47b42a`），musl static-pie，两台 `NRestarts=0`。
oleFedora01 的 `provider_stats_state` 目前 **0 行**：它自 06:07Z 起没有流量，
而缓存证据只在**会话续轮**（`session_continuation`）时才采集 —— 没有续轮就没有证据，
这是口径，不是丢数据。

**还没解决的：** 入口那台 VPS 仍是 0.3.10（需 root，见 §15.6/§15.9.1）。它既不会
落盘这份证据，也没有 `own_the_error`；在它升级之前，客户端应直指 NAS
`http://10.126.126.6:3795/v1` 或本机 `http://10.126.126.20:3795/v1`。

### 15.11 速度判据的收口：一次慢的**长**答案，不该换掉一个 99% 命中的会话（`d2a29dc`，2026-11-09 15:35 CST 部署）

**先说结论：轮换没有坏。** 「auto rotate」一直活着（12 个并发探针实测：
opencode_go_2 73 次 / volc_1 35 次 / opencode_go_1 23 次 / xiaomi、ollama_local、
minimax 各 3 次 / volc_t_1 2 次；池 12 成员 = 11 订阅 + 1 计费，3 个撞配额墙，
7~8 个健康）。但**93 % 的流量是钉住会话**（07:00Z 起 358 个请求，
`session_affinity_hits_total=365`、miss 仅 24）—— 轮换只管新会话和探针。
所以「某个 provider 看起来没被轮到」多数时候是**它只服务于几条老会话**，
不是被权重饿死。

**真正把会话赶走的是「速度逃逸」。** 会话粘在 minimax 上（上游缓存命中 99.5 %），
2026-11-09 06:00:03.745Z（0.3.12 部署后 15 秒）它恢复后第一个样本
`latency_ms=13250`；同刻 tier 中位数 = 5249（`opencode_go_1` 05:59:55 的样本，
三个成员各一个样本）。判据 `auto_pool_pin_escapable(13250, 5249)`：
`13250 > 5249×1.5` ✓ 且 `13250 > 10000` ✓ → **摘钉**。
**1.7 秒后**（06:00:05.417）该会话转给 `volc_1` → **上游 prompt cache 被丢掉**。
minimax 全程健康：04:14:11 一次 502 之后再无失败日志
（`last_failure_at=04:14:10.938`），行 `updated_at=06:34:09` 只是最后一次请求落盘。

> 之前的怀疑「14:57 那次部署后 4 个会话迁走」是**时间窗对错了**：迁移实际发生在
> **06:00–06:02Z（14:00–14:02 CST）**，即 0.3.12 部署（05:59:49Z）之后，不是 14:57 那次。
> 用「≤06:45Z vs ≤07:25Z」比较把整个上午都算了进去。

**根因：时长被当成了速度。** 06:00 窗口里每一个「慢」样本都是**长答案**：

| provider | 完整往返 | 输出 token | 每 1k token | 慢的是谁 |
|---|---|---|---|---|
| minimax | 13 250 ms | 1 879 | 7 051 ms | 客户端要得多 |
| volc_1 | 23 696 ms | 1 189 | 19 930 ms | 客户端要得多 |
| volc_t_1 | 19 179 ms | 1 589 | 12 070 ms | 客户端要得多 |
| xiaomi | 28 500 ms | 880 | 32 386 ms | 客户端要得多 |

整程时长和每 token 速率在 NAS 上**完全对不上**：

| provider | 整程中位数（看起来最快） | 每 1k token（真实速度） |
|---|---|---|
| opencode_go_2 | 4 101 ms（最好） | 13 950 ms/1k ≈ 72 tok/s（云端最慢） |
| ollama_local | 5 555 ms（普通） | 6 119 ms/1k ≈ 163 tok/s（最快） |
| xiaomi | 16 028 ms | 30 634 ms/1k |

**修法：时长只有在生成量无法背锅时才允许定罪。**

1. **有速率证据** → 用 `auto_pool_decode_is_slow`（`38e85ad` 已有）：整程时长只当
   「触发器」，判决看每 token 速率。
2. **无速率证据**（答案太短测不出速率）→ 只有当
   `provider_stats::unattributable_latency_ms(provider)` 存在时才用时长定罪。
   这个新函数取窗口内 **warm 且 `tokens_out < 256`** 的样本中位时长 ——
   **短答案的时长里没有生成量可背锅，warm 又排除了另一个嫌疑（冷启动重读全文的
   prefill）**，剩下的才是成员自己的时延。
3. **两者皆无** → 不摘钉。*没有证据表明它慢，不等于它慢*；轮换本身就是用来测量它的。

于是 06:00 那个样本（1 879 token ≥ 256 ⇒ 不可归因 ⇒ 无速率证据 ⇒ 不摘钉）被保住，
而集成测试里那个只回 5 token 的慢 mock（短且 warm ⇒ 可归因 ⇒ 时长定罪）行为不变，
`test_auto_pool_escapes_a_pin_that_is_slow_enough` **无需改动即通过**。

**自我强化的饥饿（解释了为什么 minimax / ollama_local / xiaomi / volc_t_1 没有
`provider_stats_state` 行）：** 没有会话钉住 → 没有续话样本 → `cache_evidence=None`
→ 只拿保底权重 → 继续饿。opencode_go_1/2/volc_1 有 98~99 % 是因为它们一直在
被钉住的会话供着。

**顺带挖出来的坑：body 里的 session id 是死代码。** `select_provider` 在
`gateway.rs:3511` 调用，而 body 的 session id 在 `:4017` 才解析，所有
`buffer_request_body_chunk` 调用都在 **≥:4029** —— `ctx.request_body_buf` 在
4017 **恒为空**，`resolve_from_body` 是死代码。文档里写的
`metadata.user_id` / `client_metadata.session_id` / `prompt_cache_key`
**三种写法全部无效**。实测：只带 body 的 4 个请求 → **4 个不同 provider**
（minimax→xiaomi→volc_1→opencode_go_1），session_id 列为空；只带请求头的 4 个
→ 全在 volc_t_1。真实客户端的 id 来自**请求头**
（`x-session-id` / `x-conversation-id` / `session-id` / `thread-id`），所以它们的
钉住是生效的。

**改动与部署。**

- `crates/maxllm-gateway/src/provider_stats.rs`：新增 `unattributable_latency_ms`
  （warm + `tokens_out < 256` 的中位时长）+ 4 个单元测试；
- `crates/maxllm-gateway/src/gateway.rs`：`auto_pool_pin_is_slow` 提成自由函数并按上面
  三档重写 + 5 个单元测试（未测量留、长答案留、短答案走、每 token 慢走、快回合留）；
- 门禁：`cargo fmt` / `cargo test --workspace`（913 passed, 0 failed）/
  `cargo clippy --all-targets -- -D warnings` 全绿；
- 部署：NAS `10.126.126.6` 与 oleFedora01 `10.126.126.20`，sha `d98cba34cfbc6e2f`
  （`d2a29dc`），musl static-pie，两台 `active` + `/health` 200。

**还没解决的（已量化，未做）：**

- 入口 VPS 仍是 0.3.10（需 root，见 §15.6/§15.9.1）；
- **body 里的 session id 是死代码** —— 要么在选路前缓冲 body，要么把文档里那三种
  写法删掉；
- per-provider 读超时（ollama_local 有 124 s 挂起）；
- 半开成员仍占轮换槽；探针失败不计入熔断；
- `auto_home` 池 14 个成员里 11 个常死。

### 15.12 池子自己消化容量：三件套（停顿预算 / 配置热加载 / 逃逸目标）（`3c0fefe`，2026-11-09 15:5x CST 部署）

§15.9 解释了 11:15 那次为什么"没转"：rotate 是活的，但那一轮**没有可轮换的候选**
（智谱是那个模型唯一的 provider），请求又落在还没升级的入口 VPS 上。本节把
"池子自己消化容量"这件事做实：池子的成员**上游卡住**、**运营改配置**、**逃逸时
选谁**这三件事，网关自己兜住，不等客户端重试、不等人工重启。

#### 15.12.1 per-provider 停顿预算：`read_timeout_secs`

**问题。** 路由级 `timeout_secs = 120` 是"整个响应"的预算，上游接了连接却不说
话，客户端就被挂满 120 s。NAS 上 `ollama_local` 实测挂过 **124 s**（TTFB p99
30 s）——这不是"慢"，是"哑了"，而哑掉的成员还在轮换池里占着会话。

**做法。** `ProviderConfig::read_timeout_secs`（秒）；peer 的读预算改成

```rust
peer.options.read_timeout = Some(
    provider.read_timeout.or(route_timeout).unwrap_or(DEFAULT_READ_TIMEOUT /* 60s */),
);
peer.options.write_timeout = Some(route_timeout.unwrap_or(Duration::from_secs(30)));
```

**语义已核实：pingora 的 `read_timeout` 是「每次读操作」的超时，不是整个响应的
总时长。** 所以缩短它**只砍"上游卡住"**，不砍长回答 —— 长回答的两次 delta 之间
本来就远小于 read_timeout。写超时仍然走路由预算（写的是请求体，不是响应）。

**failover 已核实。** `ReadTimedout` 在 `error_while_proxy` 的 `starved` 集合里
（`ConnectionClosed | ReadError | ReadTimedout | WriteError | WriteTimedout`），
所以读超时属于"这个成员哑了"而不是"这个 provider 挂了"：池内**重放**到下一个
成员，同时记一次熔断失败。`gateway.rs:6515`。

**NAS 上线（热加载生效，无需重启）。** 12 个 auto 池成员加 `read_timeout_secs`：
云端 60 s（`volc_1 volc_2 volc_t_1 minimax xiaomi zhipu_1 deepseek kimi
opencode_go_1 opencode_go_2`）、`ollama_local` **45 s**（实测挂过 124 s）。
备份：`maxllm.toml.bak-<ts>-stallbudget`。

**端到端测试（新增）。** `crates/maxllm-gateway/tests/integration.rs`：
`test_read_timeout_budget_stops_a_stuck_upstream_and_fails_over` —— provider 配
`read_timeout_secs = 1`、mock 上游延迟 5 s，断言请求**没有等满 5 s**，而是按停顿
预算放弃、failover 到 backup provider 并返回 200（`X-MaxLLM-Provider: backup`、
`X-MaxLLM-Fallback-From: stuck`）。实测耗时 **1.17 s**（改判据前这一条要等 5 s）。

#### 15.12.2 配置热加载加固：半写的 `maxllm.toml` 不再让新池静默失效

**问题。** 旧的热加载逻辑有两个洞：① inotify 事件在 **1 秒窗口内被丢弃**；
② **首次解析失败就放弃**。于是运营用"非原子写"改配置（先截断、再补齐）时，
网关正好读到那个空文件 → 解析失败 → **新池/新 provider 静默不生效**，而监控上
一切正常。8 小时内实测 **3 次** `TOML parse error at line 1, column 1`。

**做法（`crates/maxllm-gateway/src/main.rs`）。**

- **settle**：最后一个事件后 **800 ms** 才读文件；
- **重试**：解析不过就重读，最多 **5 次 × 250 ms**（`read_config_settled`）；
- **30 s 轮询兜底**：`config_stamp` 比 mtime/size，事件丢了也能发现；
- **两个指标**：`maxllm_config_reloads_total` / `maxllm_config_reload_failures_total`。

**实测通过。** 用非原子写入（截断 → 150 ms → 补齐）改 NAS 的 `maxllm.toml`，
网关 `07:51:44Z` 成功重载：`config_reloads_total 1`、`failures 0`、**无重启**。

#### 15.12.3 逃逸目标不落到另一个慢成员 + 扫描不占探针槽

**问题。** `auto_pool_warm_target` 过去用 `is_open()` 扫描候选 —— 每扫过一个
**半开**成员就泄漏一个探针槽，于是那些成员卡在半开、对外一律 503；而且逃逸
时可能从一个慢成员逃到**另一个同样慢**的成员。

**做法。** 扫描改用**只读**谓词 `allows_traffic`，选中的那一个才用 `is_open()`
去**预留**半开探针槽；目标排序新增**每 token 速率**优先级：不慢的成员优先于慢的
成员，**全池都慢时仍然会选一个**（不回归成"不逃逸"）。

**测试。** `the_escape_prefers_a_fast_member_over_a_slow_one` /
`the_escape_still_lands_when_every_member_is_slow`（2 个新单测）。

#### 15.12.4 部署与门禁

- **NAS `10.126.126.6` 与 oleFedora01 `10.126.126.20`：sha `bbb187f147d53182`
  （`3c0fefe`），musl static-pie，两台 `0.3.12` + `active` + `/health` 200 +
  `NRestarts=0`，两台 sha 一致**（上一轮只上了 NAS，本轮补齐 Fedora）**。
- 门禁：`cargo fmt` / `cargo test --workspace`（919 passed, 0 failed）/
  `cargo clippy --all-targets -- -D warnings` 全绿。

#### 15.12.5 NAS 实测快照（2026-11-09）

| 指标 | 数值 |
|---|---|
| TTFB（38 031 条 client_response） | p50 **1 849 ms** / p90 5 303 / p95 8 048 / p99 19 203 |
| 客户端断开 | 5 078 次，**全部是 agent 循环主动取消**，不是首字节超时 |
| 速度谱系（整程 ms/1k，中位） | `ollama_local` 7 875（127 tok/s）< `minimax` 10 575 < `opencode_go_1` 10 751 < `volc_1` 13 623 ≪ **`xiaomi` 27 792（36 tok/s，慢 3.5 倍）** |
| 缓存命中率（token 加权） | `opencode_go_1` 97.9% / `opencode_go_2` 97.8% / `volc_t_1` 97.1% / `minimax` 97.0% / `volc_1` 96.6% / `xiaomi` 96.5% / `ollama_local` 94.9% |
| 会话粘性 | 33 个 session，**6 个大 session 占 96% 请求**，每个跨 4~6 个 provider（迁移时刻与上游故障/配额墙吻合）；`session_affinity_hits_total 117 / misses 1` |
| 8 h 故障全景 | 1 855 次客户端断开（正常）/ **97 次 `breaker sync poll failed peer=http://10.126.126.199:18081 error=HTTP 404`**（VPS 还没升级）/ 28 次 429 / 6 次 ConnectTimedout / 11 次 Upstream WriteError / 2 次截断 |

#### 15.12.6 仍然未解决（已量化）

- **入口 VPS 仍是 0.3.10**：`/admin/breakers` 返回 **404**（老二进制没有这个端点
  ⇒ 不认 `[quota_parking]`、不参加跨节点 breaker 共享），也没有 `own_the_error`
  ⇒ 客户端打 `:18081` 仍会拿到上游 429 原文。15:5x CST 复测：两台的
  `/v1/responses/zhipu_1` 都已返回网关自己的 503
  （`zhipu_1=circuit open (quota force-open)`），`/v1/responses/auto` 两台都 200
  （VPS 那次还带 `X-MaxLLM-Fallback-From: kimi`，说明 failover 活着）—— 但
  **老二进制只有在撞墙当下**才会把原文交给客户端。需要 root：
  ```bash
  REMOTE=root@10.126.126.199 SOURCE=oliveagle@10.126.126.6 \
    ~/.agents/ops/machines/vm_tecent_cloud_01/maxllm/deploy.sh
  REMOTE=root@10.126.126.199 \
    ~/.agents/ops/machines/vm_tecent_cloud_01/maxllm/deploy-nginx.sh
  ```
  在那之前，客户端 base_url 直指 NAS `http://10.126.126.6:3795/v1` 或本机
  `http://10.126.126.20:3795/v1`。
- **body 里的 session id 是死代码**（要么选路前缓冲 body，要么删文档那三种写法）。
- **TTFB 专项**（首字节 deadline）还没有 —— 现在的 read_timeout 是"每次读"，
  一个**每 30 s 才吐一个 token** 的上游仍能占住客户端很久。
- 半开成员占轮换槽（warm_target 侧已修，rotation 侧仍可查）。
- 探针失败不计入熔断；`auto_home` 池 14 个成员里 11~14 个常死。
- `open_design` 还没配 `read_timeout_secs`（脚本没匹配到那段配置）。

### 15.13 池子不该忘记自己学到的速度（`29ba313` + `a762389`，2026-11-09 16:21 / 16:38 CST 部署）

**缺陷。** 轮换按"每 token 速度"给成员分槽（`auto_pool_speed_factor`），但速度的证
据有两个都会归零的窗口：样本窗口 **1 h**（`DECODE_RATE_WINDOW`），而持久化只存
**10 min** 的 `total_latency`（`TOTAL_LATENCY_WINDOW`）。⇒ 成员安静一会儿、或网关重
启一次，就从"被测过的慢成员"变回"没测过的新成员"，拿到和最快成员**一样的份额**。

**今天早些时候的实测（NAS，修复前）。** 一次部署（重启）把速度证据全清零，紧跟着
12 个全新会话铺到 7 个可用成员：`volc_1`×3、`opencode_go_2`×2、`xiaomi`×2、
`ollama_local`×2、`volc_t_1`×1、`minimax`×1、`opencode_go_1`×1 —— `xiaomi` 吃到 2 个，
和最快成员同份额，尽管它比全池**慢 2.5~3.5 倍**（27 792 ms/1k ≈ 36 tok/s，对全池中位
10 575）。这就是"池子把自己的学习成果忘了"：慢成员重新拿到快成员的份额，端到端延迟
跟着回退。

**修法（三层）。**

1. **记忆**（`LAST_SPEED`）：每个成员最后一次被测到的速度（ms/1k）+ 样本数 + 测量时刻。
   成员持续服务时每 **30 s** 刷新一次（`SPEED_MEMORY_REFRESH_INTERVAL`），有效期
   **24 h**（`SPEED_MEMORY_TTL`）。样本会老，**结论不该跟着老**。
2. **持久化**：`provider_stats_state` 加 3 列（`speed_ms_per_1k` / `speed_samples` /
   `speed_age_secs`）。老库**就地迁移** —— `CREATE TABLE IF NOT EXISTS` 永远不会改
   已存在的表，所以光改建表语句不够，必须 `ALTER TABLE ADD COLUMN`（照抄
   `migrate_request_log_columns` 的写法），否则每存一次都报
   "no column named speed_ms_per_1k"。
3. **只用于轮换权重**（便宜决策）。逃逸与"钉住会话要不要换人"仍要**新鲜证据**（贵决
   策）：拿一条过期的判决去扔掉一个还热着的 prompt cache，代价比多给慢成员一两个槽大得多。
   "**从没测过**"仍然是 `None` —— 新加进池子的账号照样拿流量被测量（用户还要往池子里
   加账号，这条路不能堵死）。

**本节自己也踩了一个坑，是生产测出来的，不是想出来的。** 第一版把"刷新间隔"记在
**尝试**上而不是**成功**上：成员凑不够 4 条合格样本时，第一次尝试落空，却把 30 s 的
间隔用掉了，于是后面真正够格的样本全落在间隔里 —— 成员**永远不被记住**。NAS 实测：
`deepseek` 在 50 s 内答了 6 次合格回合（warm + ≥256 输出 token），池子**一个数都没记
住**（`speed_ms_per_1k = NULL`），它于是被当成新成员。修法：只有**算出了**速度才开
始计时；落空的尝试下一次立刻重试。回归测试 `a_bursty_member_is_remembered_too`
（去掉修复即红）。

**测试。**

| 测试 | 断言 |
|---|---|
| `the_remembered_speed_outlives_its_samples` | 证据过期后记忆仍在；`forget()` 后消失 |
| `a_never_measured_member_has_no_remembered_speed` | 没测过的成员仍是无记忆（要被测量） |
| `the_remembered_speed_comes_back_from_the_store` | 记忆能从库里回来（带年龄），超过 24 h 的丢弃 |
| `the_snapshot_carries_the_remembered_speed` | 只有记忆的成员也有快照行（能落库） |
| `a_bursty_member_is_remembered_too` | 突发流量的成员也被记住（**去掉修复即红**） |
| `an_old_database_gains_the_speed_columns_in_place` | 旧库就地迁移出 3 列，老行不丢 |
| `the_speed_factor_uses_the_remembered_rate` | 轮换因子用记忆速率；未测成员仍是 1.0 |
| `a_known_slow_member_keeps_its_smaller_share` | 慢成员的槽位明显少于快成员（**去掉修复即红**） |

**NAS 生产实测（`10.126.126.6`）。**

| 观察 | 数值 |
|---|---|
| 迁移 | 部署后 `provider_stats_state` 立刻出现 3 列，老行原样保留（`speed_ms_per_1k = NULL` = 未测过） |
| 记忆跨重启 | 16:38 重启后 `opencode_go_1` 仍是 **10 297.0 ms/1k / 81 样本** —— 值与样本数都是**记忆里的**；重算的话会用恢复回来的 10 min 窗口（34 条），不是 81 条 |
| 记忆比证据活得久 | `opencode_go_1` 的记忆 age 173 s，而它最后一条新证据是 156 s 前 —— 证据停了，记忆没停 |
| **证据归零，记忆还在** | `deepseek` 安静下来后逐分钟观测：合格新样本 9 → 4 → 1 → **0**，记忆 9 888.0 ms/1k 的 age 一路 19 s → 322 s → 625 s → **747 s** 仍在（16:51–16:52 CST）。旧版本这里会是 `NULL` —— 池子会把它当新成员，给它和最快成员一样的份额 |
| 修 bug 前后同一成员 | `deepseek` 同样的 6 次合格回合：修复前 `NULL`，修复后 **9 887.8 ms/1k / 7 样本** |
| 现存记忆（16:52 CST 快照） | `deepseek` 9 888 / `opencode_go_2` 10 081 / `opencode_go_1` 10 600 / `volc_t_1` 14 545 ms/1k |
| 速度谱系 | 最快的 `deepseek` 9 888，最慢的 `volc_t_1` 14 545 —— 慢 **1.47 倍** → 轮换因子 **0.75 vs 1.25**（慢成员拿 8/51 的槽，快成员拿 13/51） |

**部署与门禁。**

- NAS `10.126.126.6` 与 oleFedora01 `10.126.126.20`：**sha `cf217b39f6043add`**（`a762389`），
  musl static-pie，两台 `0.3.12` + `active` + `/health` 200 + `NRestarts=0`，两台 sha 一致。
- 门禁：`cargo fmt` / `cargo clippy --all-targets -- -D warnings` / `cargo test --workspace`
  （**927 passed, 0 failed**）。

**仍然未解决（已量化）。**

- **入口 VPS 仍是 0.3.10**：新的墙（配额、熔断、记忆）在 VPS 上一样不生效 ——
  `/admin/breakers` 404、不认 `[quota_parking]`、没有 `own_the_error`。需要 root：
  ```bash
  REMOTE=root@10.126.126.199 SOURCE=oliveagle@10.126.126.6 \
    ~/.agents/ops/machines/vm_tecent_cloud_01/maxllm/deploy.sh
  REMOTE=root@10.126.126.199 \
    ~/.agents/ops/machines/vm_tecent_cloud_01/maxllm/deploy-nginx.sh
  ```
- **记忆只管轮换，不管逃逸**：一个慢成员安静之后仍会回到快队列里（它必须回来才能被
  重新测量），只是拿到的槽位少了。真正的"逃逸"判据仍要新鲜证据。
- **采样门槛**：速度证据要求 **warm + ≥256 输出 token + 4 条**。短答案的成员（agent
  循环里大量 2 token 的应答）永远进不了速度谱系，只能靠 `total_latency` 判。
- `volc_1` 现在被配额墙停放到 18:10（`force_open_until` = 2026-11-09T10:10:09Z），它回来后会被重新
  测量 —— 这正是本节要保住的场景。
- **入口的 `/admin/breakers` 回答的不是 VPS 自己**：`:18081/admin/breakers` 返回的 32 条断路器状态
  与 NAS 的**逐条相同**（`zhipu_1` 的 force-open 时刻一致）—— 说明 nginx 的 upstream 链把这个路径
  交给了 NAS，而不是 VPS 的 0.3.10。所以入口这条路也拿不到 VPS 的墙，VPS 还是得升级。
- 其余未解决项见 §15.12.6（body session id 死代码、TTFB 首字节 deadline、
  探针失败不计入熔断、`auto_home` 常死成员、`open_design` 未配 `read_timeout_secs`）。
  "半开成员占轮换槽"已查清并修掉：**不是轮换扫描的锅**（`find` 停在第一个准入成功的
  候选上，拒绝时计数不变），是 **failover 路径**把探针槽带走了 —— 见 §15.14。

### 15.14 半开的探针槽属于请求，不属于 provider（`bcebb86`）

**缺陷。** 熔断器半开时只允许**一个**探针请求（`half_open_max = 1`）。这个槽在请求
被**准入**到该成员时占下，而归还只有两个地方：① 熔断器自己记outcome 时（5xx、连接
失败、429、成功）；② 请求结束时 `logging` 归还**最终服务的那一个** provider。

但有一类失败**不记 outcome**：`401/403/404/405/408/409/410/425`，以及 provider 不
carry 该模型时的 `400/422` —— 它们**直接 failover** 到下一个 provider（`should_failover_status`）。
于是失败的 provider **把槽带走了**：它停在"半开 + 槽被占"，此后每一个请求都被判
"circuit open"拒掉，直到staleness reaper 回收（`2 × fail_timeout`，且下限 120 s）。

**后果。** 一个**活着**的成员（它刚刚回话了，只是回的是确定性拒绝）悄悄退出了轮换，
池子在最需要容量的时候少了一个人。而且它不是"慢"，是"直接不接"。

**不是轮换扫描的锅。** 遗留项写的是"半开成员占轮换槽（warm_target 侧已修，rotation 侧仍可查）"，
本节查下来 rotation 侧**没有**这个泄漏：轮换用 `find(|e| admits(e))`，`find` 停在第一个
准入成功的候选上，而 `half_open_admission` 拒绝时是 `fetch_add` 之后立刻 `fetch_sub`（计数
不变）。warm_target 侧那种"看一圈每个成员都占一个槽"的泄漏在 `3c0fefe`（§15.12.3）已经修掉。
真正漏的是 **failover 路径**：请求离开了 provider，却没把槽带走。

**修法。** 在 `switch_to_fallback` 里，请求**离开**那个 provider 时把槽还回去；而且
**只在它仍是半开时**还 —— 被熔断器记过 outcome 的失败（5xx/连接失败/429）已经自己把槽收
走并重新打开了熔断器，所以状态判断顺带避免了一次重复归还。

```rust
// The failed provider may still be holding the half-open probe slot
// this request reserved when it was admitted to it. ...
if p.circuit_breaker.state() == crate::circuit_breaker::STATE_HALF_OPEN {
    p.circuit_breaker.release_probe();
}
```

**测试（去掉修复即红）。**

| 测试 | 断言 |
|---|---|
| `test_a_deterministic_rejection_returns_the_half_open_probe_slot` | 成员跳闸 → 被探 → 回 401 → failover；**紧接着的下一个请求必须还能探到它**（修复前它被跳过，客户端拿到的是 backup 的回答） |

**门禁。** `cargo fmt` / `cargo clippy --all-targets -- -D warnings` 干净；
`cargo test --workspace` **928 passed, 0 failed**（基线 927 + 新增 1）。

**部署。** NAS `10.126.126.6` 与 oleFedora01 `10.126.126.20`：sha256[:16] =
**`51cd3cb6dd98993f`**（`bcebb86`），两台 **0.3.12** + `active` + `NRestarts=0` +
auto 池 9/12 可用，sha 一致。

### 15.15 首字节 deadline：上游"接了请求但不开口"要在 30 s 内换人（`feat/capacity-resilience`）

**症状（生产证据）。** 2026-11-09 17:0x 前后，NAS 的 recordings 里有三条请求
`ttft_ms` = 95.9 s / 124.9 s / 127.8 s，两条以
`Downstream ConnectionClosed … Prematurely before response body is complete` 收尾
—— **客户端先放弃了**（客户端的耐心大约 120 s），网关的 failover 恰好在 125 s 落地，
晚了 5 s。

**根因（读 recorder 的事件序列，不是猜）。** 这三条的完整序列是：

```
t+0.0s   provider_request → opencode_go_1 / opencode_go_2
t+120.3s error: Upstream ReadTimedout … while reading response headers   ← 上游一个字节都没给
t+120.3s provider_request → volc_1                                     ← failover 换人
t+124.6s provider_response 200 (volc_1)                                ← 4.6 s 就答了
t+125.1s client_response: Downstream ConnectionClosed                   ← 客户端 120 s 的耐心已尽
```

三个事实：

1. **慢的不是网关，是上游"不开口"**：`while reading response headers` —— 请求送到了，
   上游 120 s 一个响应头都没回。
2. **failover 本身是好的**：换到 volc_1 后 **4.6 s** 就答了。问题是它**落地太晚**。
3. **落地太晚的原因是"停顿预算"太长**：这两个 provider 没配 `read_timeout_secs`，
   于是 `provider.read_timeout.or(route_timeout).unwrap_or(DEFAULT_READ_TIMEOUT)`
   取到了**路由的 120 s** —— 正好等于客户端的耐心，所以 failover 永远来不及。

**为什么不能用"更小的 read timeout"代替。** `read_timeout` 是**每次读**的预算，
它管的是"上游中途哑了"；把它调小到 30 s 会把**长思考的流**（两个 chunk 之间停 40 s）
也一起砍掉。首字节 deadline 管的是**开始**：上游一旦开口，后面流多久都行。

**24 h 分布（决定阈值用）。** NAS 24 h 共 29 316 条：

| 分位 | p50 | p90 | p99 | max |
|---|---|---|---|---|
| TTFT | 1.9 s | 5.2 s | 14.4 s | 130 s |

慢尾（ttft > 30 s）只有 **87 条（0.3 %）**，而且**与上下文大小无关**（0–25 k 与
200 k–250 k 两个桶的 p50 分别是 1.1 s 和 2.1 s）—— 说明慢尾**不是"大上下文 prefill 慢"**，
而是**上游真的停住了**；而池子里别的成员 p50 只要 1.9 s（prompt cache 让 prefill 几乎免费），
换人几乎总是更快。按 provider 拆，"上游 45 s 不开口"的 23 条里 **opencode_go_1 占 10 条、
opencode_go_2 占 6 条**。

**修法。** 每个 provider 一个 `first_byte_timeout_secs`（默认 30 s，`0` = 关闭）：
上游必须在 30 s 内交出**第一个字节**，否则这次尝试作废、failover 换下一个成员，
并把这次失败记进熔断器（那个成员会被 circuit open 踢出轮换）。

**只在流式请求上生效。** 流式请求的首字节是**第一个 token**（正常成员 1.9 s）；
非流式请求的首字节是**整个答案**，用 30 s 去卡它会把合法的长生成砍掉。判定"是不是流式"
只能在**读完请求体之后**（body 里的 `stream`），而上游请求头是在读 body **之前**写出去的
—— 两者的时序对不上。

**所以判定结果用一个 cell 传递**（`vendor/pingora-proxy/src/first_byte.rs`）：

* `ProxyHttp::first_byte_budget()` 把 cell 交给 proxy；默认 `None` = 不设 deadline。
* 网关在 request body filter 读完 body 后决定：流式 → `Some(30 s)`，非流式 → `None`。
* proxy 的上游循环**先等这个决定**再开始读响应 —— 已经在读的那次读不能被打断，
  否则**读了一半的响应头会丢**。等待期间请求体照常流向上游（body filter 不能被堵死）。
* 决定到了才给**第一次读**套上超时；**响应头不算答案**，所以 deadline 一直挂到
  **第一个 body chunk** 到达为止，之后流多久都不管。超时按 `ReadTimedout` 上报 ——
  网关把它当成"这个成员不会答"，照常 failover + 记熔断。

**测试（去掉修复即红）。**

| 测试 | 断言 |
|---|---|
| `test_first_byte_deadline_cuts_a_slow_start_and_fails_over` | provider `first_byte_timeout_secs = 1`、`read_timeout_secs = 60`，上游 5 s 才开口 → **~1 s 内 failover** 到 backup 并 200；去掉修复则等到 5 s 才回（provider 仍是 slow） |
| `test_first_byte_deadline_does_not_cut_a_non_streaming_answer` | 同样 1 s deadline，但**非流式**请求、上游 2.5 s 才答 → **不被砍**，答案来自 slow（不发生 failover） |
| `test_first_byte_timeout_defaults_to_unset_and_is_parsed` | 配置：`first_byte_timeout_secs` 未设 = `None`，写了 = `Some(20)` |

**门禁。** `cargo fmt --all -- --check` 干净；`cargo clippy --all-targets -- -D warnings`
退出码 0；`cargo test --workspace` **931 passed / 0 failed**（基线 928 + 本节 3 个新测试）。

**部署。** commit `2fe7820`（代码）+ 本节（文档），`scripts/deploy_maxllm.sh nas && … fedora`
从 `/tmp/mx-capacity` 这棵工作树出发（**不要**从主工作区发），musl static-pie 守卫通过：

| 节点 | sha256[:16] | version | health | systemd | NRestarts |
|---|---|---|---|---|---|
| oleNas01 | `96081d90f961bcb1` | 0.3.12 | 200 | active | 0 |
| oleFedora01 | `96081d90f961bcb1` | 0.3.12 | 200 | active | 0 |

两台 sha 一致。落地后二进制的 strings 里有 `no upstream byte within the first-byte deadline`
和 `first_byte_timeout_secs`（说明新代码真的在跑）。冒烟：`/v1/responses/auto` 流式请求
两台都是 **200**，TTFT 0.50 s（NAS）/ 0.47 s（Fedora）。**配置不用动** —— 未设
`first_byte_timeout_secs` 就是默认 30 s；重启后的熔断状态：17 closed / 12 half-open /
3 open（`volc_1`、`volc_2` 至 2026-11-27、`zhipu_1` 至 2026-11-10 22:58:54 +08:00 —— 
正是智谱周墙）。

**上线后的观测口径。** 注意口径：deadline 是**每个上游尝试** 30 s，failover 换人之后
客户端还要再等换手的那一次，所以客户端看到的 TTFT 落在 **30 s + 接手成员的 TTFT**
（≈ 32–35 s），而不是掉到 0。因此看的是：

* **客户端 TTFT > 60 s 的条数应当掉到 0**（原来最坏 130 s）；
* 被 deadline 换掉的那一次尝试会以 `X-MaxLLM-Fallback-From` 出现在日志里（被换掉的是
  provider，不是客户端）；
* `maxllm_requests_total{status=~"5.."}` **不应上涨** —— 换人成功时客户端仍然是 200，
  这正是本节的目的（原来客户端等不到就自己断了）。

### 15.16 `auto_home` 池：14 个成员里 11 个的 GPU 端点根本没在跑（基础设施，不是网关）

**测法。** 按配置里每个成员的 `base_url` 逐个探 `/v1/models`（从 NAS 发起）：

| 成员 | 端点 | 结果 |
|---|---|---|
| `olefedora_gpu0_minicpmv47` | 10.126.126.20:11050 | **200** |
| `coding_home_olefedora_gpu0` | 10.126.126.20:11050 | **200** |
| `humanizer_gpu3` | 10.126.126.10:11060 | **200** |
| `olefedora_gpu{1,2,3}_minicpmv47` | .20:1105{1,2,3} | 连接被关（服务端没在跑） |
| `coding_home_olefedora_gpu{1,2,3}` | .20:1105{1,2,3} | 同上 |
| `oleworkstation_gpu{0,2}_minicpmv47` | .10:11050 / :11053 | 同上 |
| `coding_home_oleworkstation_gpu{0,2}` | .10:11050 / :11053 | 同上 |
| `oleworkstation_minicpm` | .10:11023 | 同上 |

宿主机上复核：`oleFedora01` 只有 **11050** 在 LISTEN；`oleworkstation01` 在 LISTEN 的是
11010 / 11052 / 11060 —— 其中 **11052 跑的是 hy-memory**（Tencent-Hunyuan-30B-A3B-AWQ，
不是池里要的 minicpmv47），11023 / 11050 / 11053 上没有服务。

**网关侧其实兜住了。** 客户端要 `auto_home` 时：NAS 落到健康的
`olefedora_gpu0_minicpmv47`（TTFT 0.04–0.18 s）；Fedora 每次先撞一个死成员
（connect refused 是毫秒级），`X-MaxLLM-Fallback-From` 里能看到被撞的是谁，
然后换到健康成员，客户端仍然是 **200 / 0.15 s**。所以"池子利用率"在网关这一层没有漏 ——
漏的是**端点本身**。

**但两台对同一个池的健康度报得不一样**：NAS 报 `auto_home` 3/14（12 个 half-open，
因为它自己踩过），Fedora 报 14/14（它没踩过）——`breaker_sync` 只共享**配额墙**，
不共享**失败驱动的 open**。代价很小（失败是一次毫秒级 connect refused），
但要让 Fedora 一开始就跳过死成员，得把失败驱动的熔断状态也跨节点同步 ——
那是设计取舍（"在我这儿死的成员未必在我这儿死"），先记着，不擅自改。

**真正要修的是基础设施**：这些 GPU 推理服务没在跑（可能是故意停的，比如腾显存）。
容量账上，`auto_home` 现在实际只有 **3/14** 可用；真正在扛客户端流量的是
`auto` 池（订阅池，12 个成员，**10/12** 可用 —— 停放着 `volc_2` 和 `zhipu_1`）。

## 附录 A：复现与口径

```sh
# 1) 指标
curl -s http://<gateway>:3795/metrics

# 2) 截断 vs 成功（在网关主机上，对 recordings/*.jsonl）
#    ⚠️ 不能只看 "error 非空"。pingora 的 error 前缀是 ErrorSource：
#      Downstream … = 客户端先挂断（客户端行为，不是网关/上游失败）
#      Upstream   … = 上游先挂断（这才是截断）
#    判据：status_code==200 && error.startswith("Upstream")
python3 - <<'EOF'
import json,glob
for p in sorted(glob.glob("recordings/*.jsonl")):
    for line in open(p):
        d=json.loads(line)
        if d.get("event_type")!="client_response" or d.get("status_code")!=200: continue
        e=(d.get("error") or "").strip()
        if e.startswith("Upstream"):
            print("TRUNCATED", d.get("provider"), d.get("model"), d.get("duration_ms"), e[:90])
        elif e.startswith("Downstream"):
            pass  # client abort: 客户端断开，见 §1.1
EOF
#    2026-11-08 16:05 CST 实测：4 天 9 010 条 200 里 Upstream 只有 1 条、Downstream 816 条
#    一键重算：python3 scripts/stream_outcomes.py --dir recordings [--json] [--since 2026-11-07]
#
# 2b) 复现"客户端断开"这条 error（证明它属于 Downstream 而非 Upstream）：
#     发一个流式请求，读到首字节后立刻关闭连接，然后查最新一条 client_response：
#     status 仍是 200、ttft_ms 有值、error 以 "Downstream ConnectionClosed …" 开头
# 3) auto 池 usage 丢失：同一 provider 对比直连路由与池路由的 tokens_in/out
curl -s http://<gateway>:3795/v1/responses/volc_1 \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"model":"deepseek-v4-flash","stream":true,"input":"say ok"}'
curl -s http://<gateway>:3795/v1/responses/auto \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"model":"auto","stream":true,"input":"say ok"}'
# 期望：两者都记录 tokens；实测直连 85/17，池路由 0/0，且池路由 Content-Type=json 但 body 是 SSE

# 4) 幽灵行（provider 为空、status=0）的复现：对快路径发请求后立刻 RST
#    （修复前 13/13 命中，修复后 0 行；见 §2.2）
python3 - <<'EOF'
import socket, struct, time, sqlite3
def rows():
    c = sqlite3.connect("maxllm.db")
    return c.execute("select count(*) from request_logs where provider=''").fetchone()[0]
print("before", rows())
s = socket.socket(); s.connect(("127.0.0.1", 3795))
s.sendall(b"GET /health HTTP/1.1\r\nHost: x\r\n\r\n")
s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
s.close(); time.sleep(0.2)
print("after ", rows())   # 修复前 +1，修复后不变
EOF
# 统计口径：request_logs 里过滤掉历史幽灵行 —— WHERE provider <> ''
```

口径说明：
- "响应"均指 `client_response` 记录；一次客户端请求可能因 failover 产生多条
  `provider_response`，不计入分母。
- "transport error" 必须按 `ErrorSource` 拆开：`Upstream …`（上游断 = 截断）与
  `Downstream …`（客户端断 = 客户端放弃）是两件事，混在一起统计会把客户端行为算成网关故障
  （§1.1 就是这么误判的）。窗口外客户端放弃率基线 0.4%–5%，属 agent 循环的正常中断。
- 上游截断率分母 = 该 provider 的 client_response 200 数；客户端放弃率单独一列。
- `request_logs` 里同一批行会带 `tokens_in`（客户端断开时已产生的输入 token 照样计费），
  所以"下游断开"行的 token 不为零，不能拿 `tokens_in==0` 当截断判据。

---

## 附录 B：修复实施状态（2026-11-08 下午，v0.3.8 工作树）

> 本节记录审计结论**落地情况**，与上面的"结论摘要"编号一一对应。代码已通过
> `cargo fmt --check` / `clippy -D warnings` / `cargo test --workspace` /
> `--test integration`；线上已部署并实测（见每行"验证"）。

| 审计项 | 状态 | 实现 | 验证 |
|---|---|---|---|
| P0-1 截断按 200 计成功 | ✅ 已修（暴露）+ ⏳ 未做（failover） | 新增 `maxllm_stream_truncated_total{provider}`、`maxllm_stream_truncated_before_first_byte_total{provider}`、`maxllm_streams_judged_total{provider}`；SSE 缺终止事件时补 `event: error`（Responses）或 `error`+`[DONE]`（chat）；`request_logs.error` 与日志 `truncated` 字段同步记账 | 线上 `/metrics` 已出现 `streams_judged_total`，无截断时 `stream_truncated_total` 不出现；集成测试 `test_truncated_native_responses_stream_is_booked_as_truncated` 断言 200 被记截断且客户端收到 error 事件 |
| P0-1 口径订正（本轮） | ✅ 已订正 | `Downstream ConnectionClosed` 被误读为上游截断；实测 816/817 是客户端断开、真截断仅 1 条。代码侧已核实：`error_while_proxy()` 只用 `ErrorSource::Upstream` 触发 `mark_stream_truncated()`，客户端断开不会误触流截断降级 | 复现实验（读到首字节后 1s 断开 ⇒ 逐字节相同 error）；全量重算 4 天 9 010 条 200；线上 311 judged / 0 truncated |
| P0-1 客户端放弃独立计数 | ✅ 已修 | 新增 `maxllm_client_aborted_total{provider}`：`ErrorSource::Downstream` 且 `!stream_terminal_seen` 时 +1（一次交换只记一次），并从 `usage_missing_total` 中排除；`/health` 把"半开（等探测）"从 `open_providers` 拆到 `half_open_providers`——原先 12 个只是等探测的本地 GPU 被报成 circuit-open | 集成测试 `test_client_abort_is_counted_apart_from_truncation`（裸 socket 上游开流不回包 + 客户端 socket 断开 ⇒ 恰好 1 次放弃、无截断计数）；`test_health_endpoint` 断言两张表在全新配置下为空；线上实测 `open_providers` 5 个（kimi + 4 个配额熔断）、`half_open_providers` 12 个、主动断开后 `client_aborted_total=1`；README 的 metrics 表已补 |
| P0-2 auto 池流式丢 usage | ✅ 已修 | auto 改写 body 时补解析 `stream`（原逻辑只在 `keep_request_content_length` 分支解析，auto 非 verbatim 走不到）；native Responses 流优先 `NativePassthroughStream`，不再套 chat translator（套了会破坏事件流并吃掉 usage） | 线上 `/v1/responses/auto` 实测 `Content-Type: text/event-stream` 且 `tokens_in/out` 入库；audit 前 2178/2200 零 usage，部署后新请求全部带 token |
| P0-3 成本表为空 | ✅ 已修 | 内置价表补生产渠道（deepseek-v4-flash / MiniMax-M3 / mimo-v2.6-flash / glm-5.3-flash），订阅制与本地模型显式 0/0；新增 `[costs] enabled / warn_on_missing` | 线上 `cost_usd` 已非 0（minimax 单请求 0.020459）；`production_channels_have_a_price_or_an_explicit_zero` 单测防漏配 |
| P1-4 只采不看 | ✅ 已修 | collector 拆三个 exporter：logs 按 `stream-name` 落 `maxllm_recordings` / `maxllm_logs`（OpenObserve 的 OTLP **metrics** 是按指标名分流的，`stream-name` 对 metrics 无效，已在配置里注明）；新增 dashboard + 告警，定义收敛到 `scripts/openobserve_alerts.py`（7 条，可重放）；dashboard 面板收敛到 `scripts/openobserve_dashboard.py`（幂等补面板，PUT 后回读校验——OpenObserve 的 update 接口接受 `{"v8": …}` 信封会 200 但静默丢掉 tabs，必须发裸 v8 + `?hash=`） | 三路数据实测有量：`maxllm_recordings` 49 条 / `maxllm_logs` 21 条 / `maxllm_requests_total` 5 分钟 286 样本；7 条告警 `enabled=true`；dashboard 7→11 面板（新增按 provider 截断率/截断速率、首字节前截断、脱敏计数） |
| P1-5 路由前缀静默吞掉 | ✅ 已修 | `/v1/responses/<seg>` 中 `<seg>` 既非 provider 也非池名（且非 `responses` 契约后缀）→ 404 并点名 | 线上 `POST /v1/responses/one_api_kimi` → `404 unknown provider or pool 'one_api_kimi' ...` |
| P1-5b 改名残留的持久化状态 | ✅ 已修 | 启动恢复只认当前 config 的 provider：`circuit_breaker_state` 里 provider 已不存在的行直接删除（新增 `AdminStore::delete_circuit_breaker_state`），`session_bindings` 里钉在已消失 provider 上的绑定一并删除。此前这些行永久累积（本次 246 条绑定里 182 条、38 条 breaker 行里 9 条是 `one_api_*` 旧名），而旧绑定还会让 `session_affinity_hits_total` 虚高 | 线上重启日志 `Pruned circuit breaker rows … count=9` / `Pruned session bindings … count=182`；再查两张表只剩当前 30 个 provider |
| P1-5c affinity 指标口径 | ✅ 已修 | `session_affinity_hits_total` 只在钉住的 provider **真的被选用**时 +1；钉住但不可用（熔断/已不在池/降 tier）或无绑定都记 `misses_total`。三条选择路径（路由链、auto 池、请求中途进池）统一 | 单测在 admin crate（删除语义）+ 线上指标口径核对 |
| P1-6 body 哨兵 | ✅ 已修（告警） | 选中 provider 不在池内时 `warn!` 点名，不再静默把哨兵透传给上游 | 单元/集成测试覆盖 |
| P1-7 health/探针/惰性 gauge | ✅ 已修 | `/health` 富 JSON（providers/pools/open_providers + degraded）；新增 `GET /version`；`/metrics` scrape 用 `effective_state()` 刷新 breaker gauge（不消耗 half-open 探针槽）；`X-MaxLLM-Probe: 1` 流量不计指标/日志/breaker | 线上 `/health`、`/version` 实测；`pool_health.py` 已带 probe 头并接了 5 分钟 timer |
| P2 breaker gauge 误报 | ✅ 已修 | 同上（`effective_state()`） | 线上 `/health` 的 `open_providers` 与 `maxllm_circuit_breaker_state` 一致 |
| P2 日志未轮转 | ✅ 已修 | `scripts/rotate_logs.py` + systemd timer：按大小轮转 `maxllm.log`/`maxllm.err`（O_APPEND 下可安全截断），并保留 14 天 recordings | 线上 timer 已 enable 并手动跑通 |
| P2 探针污染指标 | ✅ 已修 | probe 请求跳过 metrics / request_logs / breaker 记账 | 见 P1-7 |
| P2 `/metrics` 无鉴权 | ✅ 已修 | 默认仅 loopback 可读，`[metrics] allow_remote = true` 才放开 | 3 条单测覆盖 loopback / 远程 / 未知 peer |
| P2 recordings 落盘脱敏 | ✅ 已修 | recorder 落盘前两层脱敏：①凭据键（`authorization`/`api_key`/`token`/`password`/`cookie`… 按归一化名精确匹配，`max_tokens` 等计数不受影响）整值替换 `[REDACTED]`；②自由文本里的高置信密钥形状（`Bearer …`/`sk-…`/`AKIA…`/`ghp_…`/`xox?-…`/`AIza…`/JWT/`api_key=…`）；`redact_pii` 另清邮箱/手机号/身份证。命中条目标 `redacted: true` 并计 `maxllm_recording_redactions_total`。历史分片用 `scripts/redact_recordings.py` 原地重写（同规则、原子替换、`--dry-run`） | 8 条 recorder 单测（键/嵌套/数组/自由文本/PII/可关闭/落盘断言）+ 脚本合成数据自测；线上历史分片 dry-run 为 0 命中（32 982 行 / 920 条 body 无凭据与 PII，脚本 no-op，未重写） |
| 新增（本轮自查） | ✅ | **降级保持时间递增（反抖动）**：连续流降级若无干净窗口间隔，保持时间按 1/5/15/30 分钟递增（不低于 provider 自己的 `fail_timeout`）；窗口内 ≥4 条判定的流且 0 截断视为恢复、阶梯归零。原实现固定 `fail_timeout`=60s ⇒ 系统性截断的 provider 每 60s 被放回池子、立刻再截断 4 条，客户端每个周期都吃一次坏答案，等于没降级 | 单测 `test_repeated_stream_demotions_escalate_the_hold`（60/300/900/1800）+ `test_a_clean_window_resets_the_demotion_hold`；线上已部署 `5f8a951f…` |
| 新增（审计未列） | ✅ | **流截断率自动降级**：某 provider 在一分钟窗口内截断数 ≥4 且占比 ≥25% 时强制开闸 `fail_timeout`，路由自动走到 fallback 下一家（`maxllm_circuit_breaker_stream_opens_total` + `maxllm-stream-demotions` 告警） | 集成测试 `test_a_truncating_provider_is_demoted_and_traffic_moves_to_the_fallback`：4 次截断后第 5 个请求由健康 fallback 服务 |
| P0-2 修复后实测 | ✅ 已核对 | 按 10 分钟切片复算 `request_logs`：03:00–04:09 UTC 285 个 200 全零 usage（只有 `one_api_*` 在服务）、04:10–06:19 40%、**06:20 UTC（14:23 CST 新二进制）起 208/208 = 100%**；当日 2 785 条零 usage 里 2 778 条无 error，几乎全在 06:20 之前 ⇒ 是 §2 的历史存量（§2.1） | 修复后窗口内仅剩的零 usage 行逐条带 `Downstream ConnectionClosed`（4/4 = 客户端放弃），不是提取缺口 |
| P1-幽灵行 `provider="" status=0` | ✅ 已修 | 四个快路径把 `skip_logging` 挪到第一个 `await` 之前；`logging` 的 `active_requests.dec()` 改为只在 `active_request_counted` 时执行；新增 `is_unrouted_session()` 守卫（不写 `request_logs`、不算 `requests_total{status="0"}`，改记 `maxllm_unrouted_sessions_total`，客户端侧降 debug、内部侧保持 warn）；`request_path` 一进 `request_filter` 就落 ctx（§2.2） | 集成测试 `aborted_fast_path_probe_leaves_no_request_log` **修复前必失败**（4 条幽灵行）、修复后通过，且真实请求仍恰好 1 行；单元测试钉住判据；线上 13/13 探测复现、修复后 0 行、`active_requests` 恒 0 |
| P0-3 缓存命中采集 | ✅ 已修（采集侧） | `ctx.cached_tokens_in` + `copy_cached_tokens()` 统一解析三种形态；`extract_usage` 与 `copy_usage_tokens` 合流（原来两份手抄，缓存字段只会在一条路径上被读到）；新增 `maxllm_cached_input_tokens_total{provider,model}`（`tokens_in_total` 的子集）+ 完成日志 `cached_in` | 单测 `test_cached_input_tokens_are_extracted_on_every_shape`（chat/Responses/Anthropic/无该字段）与 `test_buffered_and_streamed_usage_agree`；线上指标已出数（§3.1） |
| P0-3 缓存折价计价 | ✅ 已修（计价侧） | 价格表新增可选 `cached_input_per_1m`（`[model_costs."…"]` 每行，`#[serde(default)]` 省略即保持现状）；`CostCalculator` 新增 `cache_rates`（exact 优先、glob 兜底）+ `set_cache_rate()` / `cache_rate_for()` + `calculate_cost_with_cache(model, in, out, cached)`（cached 钳制到 in，接口不破坏：旧 `calculate_cost()` 内部转调、cached=0）；内置 `MiniMax-M3 → $0.05`；`gateway` 按配置 `set_cache_rate` 并用 `ctx.cached_tokens_in` 计价 | 3 条单测钉住折价/钳制/glob 覆盖；线上 minimax 单请求记账从 ~$0.055 降到 ~$0.010 量级（99.5% input 按 $0.05 计）；**未配置 `cached_input_per_1m` 的模型一律按 list 计**，刻意为之（渠道缓存价未知时不低估） |
| P0-3 价格表复核 | ✅ 已核对（未改价） | `minimax`/`MiniMax-M3` 6 小时 1 290 请求、119.2M in / 0.51M out 记 $36.379，与内置 $0.30/M + $1.20/M 复算值 $36.377 一致，也与厂商公开价一致；**该数字是全 list 价 ⇒ 是上限**（缓存折价见下一行） | 反推单价 $0.3051/M 与配置单价吻合，排除重复计费/单位错位 |
| P1 熔断器探针槽泄漏（旁路发现） | ✅ 已修 | `is_open()` 兼作状态机驱动，而**过滤**路径对每个候选都调用它 ⇒ 被"看过却没选中"的 provider 各占一个 probe 槽且永不释放（`pending_probes` 不落盘）⇒ 永久 503。修法：纯读 `allows_traffic()` 走所有过滤；`is_open()` 只在选中的 provider 上调用一次；幂等钳零 `release_probe()` + `logging()` 终态归还 + 2×`fail_timeout` 兜底回收；`Closed→HalfOpen` 改 CAS | 线上 `pool_health.py` BAD 503 **16/25 → 4/25**（仅剩 4 家配额强制开闸）；`ollama_local` 定向 503 → 200 并关闭熔断；3 条新回归测试；unit 162→165、integration 124(+4 ignored) 全绿 |
| §828 异构 fallback 链的模型名 | ✅ 已修 | 400/422 `unknown model` 且该名属于本部署某 provider 时，同一 provider 带 `model_override` 重试一次：body 的 `model` 改写为该 provider 的 `default_model`；`model_override.is_some()` 并入 `is_auto_route` 使改写后的 body 走 chunked（native Responses 直传分支不改 body 的坑）；三道闸 = 先试“还持有该名”的 provider、拼错名不改写、每 provider 至多一次；新指标 `maxllm_model_rewrite_retries_total{provider}` | 线上 `/v1/responses/minimax`、`/v1/responses/deepseek` 发 `deepseek-v4.1-flash:cloud`：修复前 400 unknown model → 修复后 200（分别改写为 `MiniMax-M3`/`deepseek-flash`）；5 条新回归测试（含 native Responses 路径） |
| §828 404「模型不存在」分支 | ✅ 已修 | vLLM / miniCPM 对未知模型答 **404**（`The model X does not exist. Valid aliases: …`），旧改写只认 400/422 ⇒ 整条路由 404。改为延迟到 body 判定：`pending_model_404` + `body_reports_unknown_model()`（强/弱特征分级，端点 404 不误判）→ 命中走与 400/422 相同的三道闸 + 原地改写重试，不命中走普通 failover 且模型名不变；与 429 共用 `fail_over_from_body_filter` | 线上 `/v1/responses/{olefedora_gpu0_minicpmv47,coding_home_olefedora_gpu0}` 发 `deepseek-v4.1-flash:cloud`：修复前 404 → 修复后 200（`"model":"minicpmv47-awq"`）；拼错名仍 404；指标各 +1；4 单测 + 3 集成测试 |
| 503 谎报 “All providers are unavailable” | ✅ 已修 | `select_provider` 空结果改用 `unavailable_reason()`：逐候选点名 + breaker 状态（配额 force-open / 开闸 / 半开探针在飞 / 未配置）+ 路由路径 | 线上 `/v1/responses/{zhipu_1,volc_t_1}` → 503 `no provider available: zhipu_1=circuit open (quota force-open) — every candidate of route '/v1/responses/zhipu_1' is rejecting traffic`；`pool_health` 一轮 4 家配额 503 全部自解释 |
| §1.2 4xx/5xx 误记为 `stream truncated` | ✅ 已修 | 只有 2xx..399 的流才谈得上"截断"：native Responses 流式分支加 `upstream_status < 400` 守卫（4xx 走缓冲/规范化错误体路径）；`mark_stream_truncated()` 入口对 ≥400 直接 return false（计数器唯一记账点）；`observe_stream_chunk()` 对 ≥400 提前返回（不再往 JSON 错误体追加 SSE 事件）；`logging()` 的 `truncated` 限定 `status < 400`（日志/指标双保险） | 线上 deepseek 流式 4xx（`temperature:9`→400、`max_output_tokens:-1`→422）：body 从"JSON+追加的 gateway_stream_truncated 事件"变纯 JSON；`request_logs.error` 不再写 "stream truncated"；`maxllm_stream_truncated_total{deepseek}` 对 4xx 不再记账；正常 200 流 10039B 完整收尾无回归；新单测 1 条，unit 7/7 + integration 132/132 + fmt/clippy 全绿 |

| auto 池 latency-aware 轮转 | ✅ 已修（优化） | 整段响应 EWMA（非 TTFB）+ 档内中位数 ×2.5（5s 地板）快队 + 每 50 次 exploration；sticky 钉也走同一过滤器（钉慢 provider 不 honour、重新钉到快 provider）；新指标 `auto_pool_slow_skips_total`；开关 `[auto_model].prefer_fast`（默认 true） | 线上 44 次 auto 探针全 200、xiaomi 0 次入选、skip 计数 23 只落 xiaomi；DB 钉在 xiaomi 的会话 `01a1ba19-…` 发请求 → minimax 服务 + affinity miss +1 + 绑定重写 minimax；单测 8 + 集成 1，workspace 全绿（见 §10） |

### 仍未做（需拍板或有前置）

| 项 | 原因 | 建议 |
|---|---|---|
| `/v1/responses` primary 从 ollama 端点换掉 | 属成本/配额口径决策。**订正**：原"它是截断率最高的一家"不成立 —— `ollama_local` 上游失败率 0.00%、`one_api_ollama_local` 0.11%，全仓最低；高的是 `opencode_go_2` 7.65% / `opencode_go_1` 2.58%（§1.1） | 若换，理由只能是成本/配额；从可靠性看没有理由动它 |
| 首字节前的截断 failover | ~~收益是本批 680 例~~ **订正：只有 2/816（0.25%）** 在首字节前断开（§1.1） | **不做**。收益 0.25%，代价是首包缓冲 + 交互延迟；`stream_truncated_before_first_byte_total` 继续留着做监控即可 |
| 超大 prompt 预检 | **已用数据关（2026-11-08 19:5x CST）**：`request_logs` 全量按 token 分桶，`tokens_in ≥ 128k` 的 200 行 **4 886 条**，其中 `stream truncated` **0**、`Upstream` 错误 **0**、5xx **0**；`8k-32k`/`32k-128k` 桶同样 0。**≥128k 输入截断率显著更高不成立**（与 §1.1/§8 订正一致）。若要做预检，动机只剩上游成本/额度（长 prompt 全额输入计费），不是截断 |
| 告警真实出口 | `maxllm-http-sink` 仍是占位 URL（httpbin）：7 条告警都会触发、都会 POST 到 httpbin，但没人被通知 | `scripts/openobserve_alerts.py --destination-url <webhook>` 一条命令替换（需要用户给 webhook，见 §12.7） |
| `xiaomi` / `mimo-v2.6-flash` 留在 auto 池 | 该模型 p50 21s / p90 92s / p99 4min（成功请求最大 297s，无一条超过 300s），27 条 300s 客户端超时全部落在它身上（§1.1） | 成本/交互口径决策：移出池子，或接受 auto 池有 ~10% 的请求要等 90s+ |

## 附录 C：CLI 桥接下线 + `cmd` 识别机制逆向（2026-11-08 晚，v0.3.10）

### 结论：`cmd` 没有"魔法 token"来证明自己是 CLI

逆向 `command-code@1.88.5`（`dist/cli.mjs`，3.4 MB 单文件 bundle）得到的识别链：

| 层 | 机制 | 代码位置 |
|---|---|---|
| 路径面 | CLI 私有协议 = `/alpha/*`（`/alpha/generate`、`/alpha/agent/generate`、`/alpha/sandbox/*`、`/alpha/billing/*`、`/alpha/fingerprint/record`）；公开 API = `/provider/v1/*` | `uE="/alpha/generate"`、`ib="/alpha/agent/generate"`、`EP="/alpha/fingerprint/record"`、`cR="/provider/v1/systemone"` |
| 自称头 | `User-Agent: cli`（常量 `kk`，**每个请求都带**）；`x-cli-surface`（仅 `acp`/`rpc` 面才带，普通 CLI 不带）；`x-cli-environment: production`；`x-command-code-version` | `buildCommandApiHeaders()` / `buildCommandAuthHeaders()` |
| 设备指纹 | 首次运行上报机器指纹到 `/alpha/fingerprint/record`：`thumbmark = sha256(salt + "\0machine\0" + machineId\|MACs)`；`components` 里 machineId / MAC / OS 用户名 / 主机名 / git 邮箱全部 hash，另有 `runtime: "cli"` | `buildMachineFingerprint()` / `gatherRawSignals()` |
| 凭证 | `Authorization: Bearer <apiKey>`，key 来自 `~/.commandcode/auth.json`（`apiKey`/`userId`/`userName`/`keyName`/`authenticatedAt`），或环境变量 `COMMAND_CODE_API_KEY` | `getCommandAuthKey()` / `getCommandApiKeyFromEnv()` |
| 浏览器登录 | `https://commandcode.ai/studio/auth/cli?callback=…&state=…&client=…`，回环回调把 apiKey 落盘 | `buildCommandAuthUrl()` |
| 服务端判定 | ① 路径族决定"这是 CLI 面"；② 套餐决定"能不能用 API"（Go 计划 → `/provider/v1/*` 恒 403 `upgrade_required`）；③ `/alpha/*` 上另有反代理护栏 | 厂商侧 |

实测（用本机 key，2026-11-08 21:0x）：

- `/alpha/namespaces`、`/alpha/billing/credits`、`/alpha/billing/subscriptions`、
  `/alpha/sandbox/sessions` 用普通 `curl/8` UA 同样返回 200 —— **`User-Agent: cli`
  不是这些端点的准入条件**，它只是客户端自称；
- `POST /alpha/generate`（空 body）两种 UA 都是 403，响应体是一份
  `syspool/<user>/main/<sha256>\t<size>` 的 202 行清单（CLI 上传的内容寻址文件池索引，
  被原样回显在错误体里 —— 上游的一个信息泄漏，顺带说明 CLI 会把本机文件哈希上云）。

### 下线 `cmd_cli`（v0.3.10）

厂商自己在 `/alpha/*` 的 403 里写明："Proxy use detected. This endpoint only serves CLI.
Use Command Code provider API instead. Continued proxying of your subscription violates the TOS
and will result in account ban." —— 把包月订阅通过自建网关转出去属于厂商明令禁止的用法，
因此本项目撤掉 CLI 桥接：

- 删除 crate `crates/maxllm-commandcode/`（独立 sidecar，工作区内无其它 crate 依赖它）；
- `maxllm.toml` 去掉 `[providers.cmd_cli]` 与 `/v1/responses/cli_cmd`、`/v1/channels/cli_cmd` 两条路由；
- 线上（NAS 生产节点）停用 `maxllm-commandcode.service` 并把二进制移出 `bin/`；
- **保留** `kind = "commandcode"`（Provider API，厂商授权路径）——Go 计划下它恒 403，
  升级 Provider 计划后 `/v1/responses/cmd` 原生透传即可用，网关侧无需改动。
