# MaxLLM 网关运行审计（2026-11-08）

> 范围：NAS 生产实例 v0.3.8，`/v1/responses` 家族路由 + auto/auto_home 双池。
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
| 6 | body 哨兵 `auto` 在 `/v1/responses` 仍不可用（已知缺口未修） | 当天 11 次 404 + 2 次上游 400 由 `model:"auto"` 直接透传触发；391 次历史 404 集中在 11-07 12:00–14:00 三个小时里 | P1 |
| 7 | `/health` 太浅、池健康检查未接线；quota probe 只盯 kimi | `pool_health.py` 存在但无 timer/常驻进程；`maxllm-quota-probe-kimi` 单元仍引用改名前的 `one_api_kimi` | P1 |
| 8 | breaker 指标是惰性快照，4 个 provider 长期显示 open 但实际已过 fail window | `is_open()` 在下次请求时才 lazy half-open，gauge 直接读原子 state | P2 |
| 9 | `maxllm.err` 长期未轮转，且错误文案把 sentinel 当 provider | 23 行、全部停留在 11-04；`route '/v1/responses/auto' references unknown provider 'auto'` | P2 |
| 10 | 自检探针流量计入生产指标 | 本机 loopback 探针命中已删除路由，产生 429/404/400 并写入 request_logs | P2 |

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

---

## 3. P0-3：成本账本整体不可用

- `maxllm.toml` 中没有任何成本表配置 → `calculate_cost()` 全部落到
  `no cost entry found, returning 0`（4 天内 2,186 条 WARN，占日志 WARN 的 76%）。
- 完成日志里 `cost_usd="0.000000"` 共 6,308 条，只有 42 条非 0（opencode 自带 cost）。
- SQLite `spend_logs` 表 **0 行** → 预算 / 花费 / 告警功能实际从未工作。
- 叠加 P0-2 后，即使配置了价格表，池路由的流式流量仍会按 0 token 记账。

**建议**：补价格表（至少 6 家云 provider），或明确关闭 cost 模块并删掉 WARN 噪声；
同时把 usage 缺失率做成指标（`maxllm_usage_missing_total`）。

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

---

## 6. P1：body 哨兵 `auto` 仍然不可用

- `/v1/responses`（非池路由）收到 `{"model":"auto"}` 时，body 里的哨兵会被原样发给
  primary provider → 404，然后沿 fallback 链一路 404/400。
- 当天仍有 11 次 404 + 2 次上游 400 由它触发；历史 425 次 404 集中在 11-07 12:00–14:00。
- 设计上不给 `/v1/responses` 加 `auto_model=true` 是为了避免把显式模型静默改写
  （这个判断是对的）。**建议改成精确匹配**：只有当 body `model` 恰好等于某个池名
  （`auto` / `auto_home`）时才走池；其他模型名保持原样。这样既保留显式模型语义，
  又消除哨兵的 404 风暴。

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

---

## 8. P2：小项

| 项 | 现状 | 建议 |
|---|---|---|
| breaker gauge 语义 | `state=1` 是惰性快照；4 个 provider fail window 已过仍显示 open | gauge 输出 effective state，或加 `last_failure_at`/`force_open_until` 标签 |
| `maxllm.err` | 23 行、全部停留在 11-04，无轮转 | logrotate（按大小/天），并修掉 `unknown provider 'auto'` 的误导文案（sentinel 不是 provider） |
| 请求体落盘 | recordings 里 451 条 client_request + 388 条 client_response 存了完整 body | 脱敏（API key / 个人信息）或只存 hash + 长度；明确保留期 |
| `/metrics` 暴露 | 0.0.0.0:3795 无鉴权 | 绑定内网/加 bearer，或由 collector 走 localhost（当前已是 localhost 抓取，可直接收口） |
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

### 仍未做（需拍板或有前置）

| 项 | 原因 | 建议 |
|---|---|---|
| `/v1/responses` primary 从 ollama 端点换掉 | 属成本/配额口径决策。**订正**：原"它是截断率最高的一家"不成立 —— `ollama_local` 上游失败率 0.00%、`one_api_ollama_local` 0.11%，全仓最低；高的是 `opencode_go_2` 7.65% / `opencode_go_1` 2.58%（§1.1） | 若换，理由只能是成本/配额；从可靠性看没有理由动它 |
| 首字节前的截断 failover | ~~收益是本批 680 例~~ **订正：只有 2/816（0.25%）** 在首字节前断开（§1.1） | **不做**。收益 0.25%，代价是首包缓冲 + 交互延迟；`stream_truncated_before_first_byte_total` 继续留着做监控即可 |
| 超大 prompt 预检 | ≥128k 输入截断率显著更高 | 可先用 `maxllm_stream_truncated_total` × 请求 token 直方图确认 |
| 告警真实出口 | `maxllm-http-sink` 仍是占位 URL（httpbin） | `scripts/openobserve_alerts.py --destination-url <webhook>` 一条命令替换 |
| `xiaomi` / `mimo-v2.6-flash` 留在 auto 池 | 该模型 p50 21s / p90 92s / p99 4min（成功请求最大 297s，无一条超过 300s），27 条 300s 客户端超时全部落在它身上（§1.1） | 成本/交互口径决策：移出池子，或接受 auto 池有 ~10% 的请求要等 90s+ |
