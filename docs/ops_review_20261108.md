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
| 2 | auto 池路由的**流式**请求全部丢失 usage，token/成本账本对主流量不可用 | `/v1/responses/auto` 4,257 个响应里 4,208 个（98.9%）tokens=0；全站 65.9% 的 200 响应零 usage；`spend_logs` 表 0 行 | P0 |
| 3 | 成本表为空，`cost_usd` 恒为 0，成本告警/预算失效 | 配置无 `[cost*]` 段；6,535 条完成日志 cost=0（仅 42 条非 0）；2,186 条 `no cost entry found` WARN（占全部 WARN 的 76%） | P0 |
| 4 | 监控链路"只采不看"：无 maxllm 看板、无告警；日志全部灌进 `default` 流 | OpenObserve 20 条指标流均在更新，但 0 条 alert、2 个 dashboard 均与 maxllm 无关；collector 单 exporter 写死 `stream-name: default` | P1 |
| 5 | provider 改名造成身份断裂：指标里 38 条 breaker series vs 配置 30 个 provider；旧路由被前缀匹配静默吞掉 | 9 条 `one_api_*` 僵尸 series；`one_api_volc_2` 仍 force-open 到 2026-11-27；旧 base_url `/v1/responses/one_api_*` 落到通用路由并被错误 provider 服务 | P1 |
| 6 | body 哨兵 `auto` 在 `/v1/responses` 仍不可用（已知缺口未修）~~ **✅ 实测已不再触发**：`model='auto'` 最后一次出现在 2026-11-07T14:35Z；86 行历史里 74 行是 §2.2 已修的幽灵行、1 行是旧名前缀路由的 404 | 当天 11 次 404 + 2 次上游 400 由 `model:"auto"` 直接透传触发；391 次历史 404 集中在 11-07 12:00–14:00 三个小时里 | P1（见 §13.4） |
| 7 | ~~`/health` 太浅、池健康检查未接线~~ **✅ 已收口**：`/health` 现在回 `pools{healthy,total}` + `open_providers`/`half_open_providers`；`maxllm-pool-health.timer` 5 min 一次；quota probe 已按改名后的 `kimi` 运行 | — | P1 |
| 8 | ~~breaker 指标是惰性快照~~ **✅ 已收口**：`/metrics` 在 scrape 时输出 `effective_state()`，过期的 open 会算成 closed | `is_open()` 在下次请求时才 lazy half-open，gauge 直接读原子 state | P2（已修） |
| 9 | ~~`maxllm.err` 长期未轮转~~ **✅ 已收口**：`rotate_logs.py` 的 `DEFAULT_LOGS` 早含它，timer 每日跑（阈值 20 MiB / 留 2 代），当前 0 字节；sentinel 文案随 §6 一起消失 | 23 行、全部停留在 11-04；`route '/v1/responses/auto' references unknown provider 'auto'` | P2（已修） |
| 10 | 自检探针流量计入生产指标 | 本机 loopback 探针命中已删除路由，产生 429/404/400 并写入 request_logs | P2 |
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

## 6. P1：body 哨兵 `auto` 仍然不可用

- `/v1/responses`（非池路由）收到 `{"model":"auto"}` 时，body 里的哨兵会被原样发给
  primary provider → 404，然后沿 fallback 链一路 404/400。
- 当天仍有 11 次 404 + 2 次上游 400 由它触发；历史 425 次 404 集中在 11-07 12:00–14:00。
- 设计上不给 `/v1/responses` 加 `auto_model=true` 是为了避免把显式模型静默改写
  （这个判断是对的）。**建议改成精确匹配**：只有当 body `model` 恰好等于某个池名
  （`auto` / `auto_home`）时才走池；其他模型名保持原样。这样既保留显式模型语义，
  又消除哨兵的 404 风暴。

**✅ 现状（2026-11-08 23:0x CST 复核）**：`adopt_sentinel_model()` 已经把「选中 provider
是池成员」这一路修好（body 被改写成该条目的真实模型名，不再有 404 与 failover 风暴）；
`request_logs` 里 `model='auto'` 的行**最后一次出现在 2026-11-07T14:35Z**，此后再无——
历史 86 行里 74 行是已修的 `status=0` 幽灵行（§2.2），1 行是旧名前缀路由的 404。
剩下唯一未覆盖的分支是「选中 provider 不在池里」（只有会话被钉在池外 provider 时才可能），
现在该分支会打 WARN 点名 `provider` / `pool` **并且**不再重写 body。不改语义（不把显式
路由静默改写成池路由），因为按 §6 的精确匹配方案等于改变路由契约，而实测该分支已不再
触发。

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

### 13.4 body 哨兵 `auto`（§6）：实测已不再触发

`model='auto'` 在 `request_logs` 里最后一次出现是 **2026-11-07T14:35Z**；历史 86 行中
74 行是 §2.2 已修的 `status=0` 幽灵行、1 行是旧名前缀路由的 404。`adopt_sentinel_model()`
覆盖了「选中 provider 是池成员」的主路；剩余「选中 provider 不在池里」分支现在会 WARN
点名 provider/pool。不改写显式模型语义，因为该分支已无实际流量。

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

---

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
