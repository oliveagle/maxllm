# MaxLLM 网关日志优化分析（2026-11-14，第 1 轮巡检）

> 范围：NAS 生产实例 v0.3.21（Moonshot tool-schema 投影修复部署后）。
> 数据快照：2026-11-14 17:12 CST。
> 日志窗口：2026-11-04 04:38Z → 2026-11-14 09:12Z（10 天，207,073 行去重后）；
> recordings 窗口：2026-11-12 → 11-14（58 个分片，54,553 请求 / 48,629 client_response）。
> 口径与 [ops_review_20261108](ops_review_20261108.md) §1.1/§12.3 一致：
> `Downstream ConnectionClosed` = **客户端断开**（不是截断），`Upstream` = 真截断。
> 复现命令见附录 A。巡检每 30 分钟一轮，记录见 §10。

## 0. 结论摘要

| # | 发现 | 关键证据 | 严重度 |
|---|------|----------|--------|
| 1 | ERROR 的 **83% 是客户端断开噪声**（11,494/13,859），量级是 11-08 审计时的 14 倍：volc 一家 7,023。根因链：慢 provider（xiaomi p90=106s）→ 客户端等不及 → 断开 → ERROR 刷屏 | §1 | P1 日志治理 |
| 2 | **volc 全族 quota backoff 连续 3 天零流量**：deepseek-v4.1-flash 全量挤到 minimax/xiaomi，xiaomi p90=106s / p99=360s、client abort 7.9%；近 3 天成本 $336 的 94% 在 minimax+xiaomi | §2 | P1 成本/体验 |
| 3 | 本地节点 **11-07~09 上游断流 2,077 次**（image/ocr/asr/minicpmv47 六端口），11-10 起 ≤9/天 —— 已自愈，疑似当时重启/压测窗口，需防回归 | §3 | P2 |
| 4 | `breaker_sync` 三个 peer **每天失败 7~341 次**（窗口内 534 次 WARN）：`.6` 与 `.20` 大概率没在跑 maxllm-server，peer 清单与实际部署不符 | §4 | P2 |
| 5 | `ollama_local` 对 `deepseek-v4.1-flash:cloud` **恒定 502**：199/2,120 条 provider 响应（9.4%）全是这一个模型，每次白走一轮 failover | §5 | P2 |
| 6 | **open_design client abort 率 32.4%**（938/2,899），是全局 4.8% 的主贡献者；abort p50 仅 5.8s，与其 402（订阅耗尽）时间线吻合 | §6 | P2 |
| 7 | **kimi-for-coding 今日 client abort 24.7%**（279/1,130，平均 8.9s），quota probe 全绿（0.75s/200）—— 疑似生成慢/流停顿，列入观察 | §7 | P2 |
| 8 | ✅ 对照 11-08 审计收口项：cost 缺失 WARN 11-08 后归零；11-07~08 的 400/404 风暴（1,276 次）消失；`moonshot flavored` 400 全窗口 0 条；上游真截断率 0.11% | §8 | — |

---

## 1. P1：ERROR 的 83% 是客户端断开噪声

10 天 ERROR 分类（13,859 条）：

| 类别 | 条数 | 占比 | 说明 |
|------|-----:|-----:|------|
| `Downstream ConnectionClosed` | 11,494 | 83.0% | 客户端 hang up（§1.1 口径），**非网关故障** |
| `Upstream ConnectionClosed` | 2,077 | 15.0% | 真上游断流，全部集中在 11-07~09 本地节点（§3） |
| 其他（DNS 502 / ReadTimedout / WriteError 等） | ~288 | 2.1% | 见下 |

`Downstream` 按上游归因（sni 精确映射，见附录 A）：

| provider | 条数 | 备注 |
|----------|-----:|------|
| volc | 7,023 | 峰值 11-08（2,378）/ 11-09（4,325），之后 172~432/天 |
| ollama_local | 1,471 | 峰值 11-07（654） |
| xiaomi | 1,441 | 峰值 11-11~13（381/407/262）—— 与 volc 停摆、流量挤入同步 |
| open_design | 979 | 峰值 11-12（665），与 §6 402 时间线吻合 |
| kimi | 279 | **全部在今天（11-14）**，见 §7 |
| zhipu / opencode / minimax / deepseek | 447 | 长尾 |

recordings 侧复核（11-12→14，48,617 个 200）：

```
client abort      2,335 (4.8%)   p50 6.6s  p90 28.2s   [0 例发生在首字节前]
upstream 截断        52 (0.11%)  其中 45 例是 opencode.ai ReadTimedout（流式 60~70s 后源站停顿）
```

**根因链**：客户端断开不是随机的 —— xiaomi 上 abort p50 15.5s、open_design 上 5.8s，
都是"生成慢于客户端耐心"。volc 11-08/09 的 6,700+ 次峰值同样是 quota 撞墙期间
慢/失败响应引发客户端撤退。

**优化建议**（按性价比）：
1. **日志降噪**：`Downstream ConnectionClosed` 在网关侧降为 WARN/聚合计数
   （它从 pingora 内部以 ERROR 打出，tracing filter 可按消息前缀过滤；真上游错误不受影响）。
   10 天 11.5k 条 ERROR 里只有 ~2.4k 条值得值班看。
2. **看板视角**：client-abort 率按 provider 出 4.8% 基线 + 分位时长，
   当前全局唯一"高得反常"的是 open_design 32.4% / kimi 24.7%（§6/§7）。
3. 客户端超时与 provider 能力错配：给路由加"该 provider 的历史 p90 > 客户端超时
   阈值时降权"（§2 的 latency-aware 思路在 auto 池已落地，可推广到 fallback 链）。

## 2. P1：volc 全族 quota backoff → 流量与成本集中

`pool-health-state.json`（11-14 17:07）：`quota_backoff` 中 volc_1 / volc_2 / volc_t_1 /
volc_1_spare 全部在列（streak=3）；近 3 天 `request completed` 中 volc 系 **0 条**。

近 3 天（11-12→14）分 provider 画像（47,482 请求，合计 $336.48）：

| provider | n | p50 | p90 | p99 | cache | $ | 备注 |
|----------|--:|----:|----:|----:|------:|--:|------|
| minimax | 19,743 | 4.1s | 15.5s | 43s | 96.8% | 183.92 | 主力，健康 |
| **xiaomi** | 8,479 | **18.5s** | **106s** | **360s** | 94.8% | 133.20 | p90 是 minimax 的 7 倍 |
| opencode_go_1/2 | 12,583 | ~6.4s | ~22s | ~100s | 97.6% | 11.95 | trunc 22（opencode 源站停顿） |
| open_design | 2,899 | 6.3s | 11.9s | 33s | 98.0% | 0 | abort 32.4%（§6） |
| zhipu_1 | 2,570 | 6.5s | 23.6s | 82s | 96.7% | 7.42 | |
| ollama_local | 1,204 | 8.7s | 22.7s | 44s | 95.6% | 0 | 502 风暴 §5 |
| kimi | 1,130 | 5.7s | 17.4s | 46s | 97.9% | 0 | 今日 abort 24.7%（§7） |

**含义**：
- deepseek-v4.1-flash 的零成本通道（volc）停摆后，一切走付费通道；
  minimax+xiaomi 吃掉 94% 成本（$317/3 天 ≈ $105/天）。
- xiaomi 的 p90=106s 说明它承接的是"别的 provider 都接不住的慢请求"（fallback=true 76 次），
  或者其自身在高峰限流降速。它是 client abort 与 Downstream 噪声的最大单一来源之一。
- 11-08 审计 §14 的缓存攻坚成果保持：全部主力 provider 缓存命中率 ≥94.8%。

**优化建议**：
1. volc 配额恢复后 xiaomi 负载自然回落；**配额状态建议接 `quota_parking` 时同步推给路由
   降权而不是只熔断**（现在是非黑即白）。
2. 对 xiaomi 做**并发上限**（in-flight cap）：它的 p90 是排队产物，限并发比加长超时更有效。
3. 成本护栏：minimax+xiaomi 合计 $105/天，按此节奏 ~$3.2k/月 —— 建议给 auto 池加
   日预算告警线（OpenObserve 告警出口仍缺 webhook，见 11-08 审计 §12.7 待办②）。

## 3. P2：本地节点 11-07~09 上游断流 2,077 次（已自愈，防回归）

`Upstream ConnectionClosed`（真截断）逐日分布：

| 日期 | 条数 | 集中端口 |
|------|-----:|----------|
| 11-07 | 67 | ws/fedora 的 image/asr/ocr/minicpmv47 |
| **11-08** | **1,849** | ws:image 341、fedora:image 337、fedora:ocr 336、ws:minicpmv47 335 |
| 11-09 | 213 | 同上四端口 |
| 11-10 起 | ≤9/天 | 长尾（groot 偶发） |

六端口四天各 ~370 次的整齐分布，像是服务端（llama.cpp/sglang）同一窗口的批量断连
（重启/升级/OOM），而不是网关问题。11-10 起归零，当前无动作；
**建议**：给本地节点接 `pool_health.py` 式探针（`/health` 深检已在 11-08 §7 落地），
断流率 >1% 时把节点移出轮换，避免再出现"每端口 370 次"才被动发现。

## 4. P2：breaker_sync peer 清单与部署不符（534 次 WARN）

`[breaker_sync] peers = [10.126.126.199:3795, 10.126.126.6:3795, 10.126.126.20:3795]`，
逐日失败（`error sending request` / 404，即连不上或没有 `/admin/breakers`）：

| 日期 | 失败次数 | 主要 peer |
|------|---------:|-----------|
| 11-09 | 341 | .199(203) / .20(122) / .6(15) |
| 11-10 | 73 | .20(56) |
| 11-11 | 27 | .199(19) |
| 11-12 | 56 | .20(38) |
| 11-13 | 8 | 三家均摊 |
| 11-14 | 7 | .6(7) |

**含义**：跨节点熔断共享只在"对端真的跑 maxllm-server 且开了 admin"时才有意义。
`.6`/`.20` 大概率没在跑（或端口不对），`.199:3795` 与 `.199:18081` 的关系也需确认
（网关对外服务在 18081；3795 是另一实例？）。每家 15s 一轮空转，日志 7~341 条/天。

**建议**：逐台 `curl /admin/breakers` 核实 —— 活的保留，死的从 peers 删掉
（一行配置热更）；顺带把"连续 N 轮全 peer 失败再 WARN"做成聚合，避免每 15s 一条。

## 5. P2：ollama_local 对 `deepseek-v4.1-flash:cloud` 恒定 502

recordings（11-12→14）provider_response：`ollama_local` 2,120 条中 **199 条非 200，
全部是 502、全部是 `deepseek-v4.1-flash:cloud`**（≈25/天）。日志侧对应
`answered 502; retrying on minimax` 的 failover 链。

这是"零成本通道优先"策略的固有税：ollama_local 不挂 `:cloud` 模型，每次尝试必 502。
502 属 5xx，breaker 似乎没有因此把 ollama_local 对 `:cloud` 的组合持久半开
（否则不会恒定 25/天）。

**建议**：在 route/fallback 配置里把 `deepseek-v4.1-flash:cloud` 的 ollama_local
条目直接摘掉（零成本意图改用显式 `no_charge` 路由），或让 breaker 对
"同模型同 5xx"做组合级熔断 —— 前者一行配置，立刻省 25 次/天的白转。

## 6. P2：open_design —— 402 与 32.4% client abort

- 402（订阅耗尽）逐日：11-09: 41 → 11-10: 40 → 11-11: 6 → 之后 0。
- 但 recordings 里 open_design 的 **client abort 率 32.4%**（938/2,899，全局第一），
  abort 时长 p50 5.8s / p90 12.2s —— 与其 p50 6.3s 的完整请求时长相近，
  即"快答完/答到一半客户端就走"。

**解读**：402 高峰期客户端拿到的是 failover 后的慢响应；402 消失后 abort 仍在，
更像 open_design 订阅受限后的**降速/截流**（或 SSE 停顿）。它是 Downstream 噪声
11-12 峰值（665 条）的来源。

**建议**：订阅状态恢复前，把 open_design 在 auto 池的权重临时调低
（latency-aware 轮转会自动做一部分）；并给 402 率加告警（同 §2-3 的告警出口待办）。

## 7. P2 观察项：kimi-for-coding 今日 client abort 24.7%

今天（11-14，至 09:12Z）kimi：1,130 条 client_response 中 **279 条 abort（24.7%）**，
全部是 `Downstream ConnectionClosed`，平均时长 8.9s；provider_response 侧 1,181 条中
仅 6 条非 200（4×429 / 2×400）。quota probe（3601 次探测）除 11-13 10:00~16:00
的 820 次 429 墙外全绿，当前 0.75s/200。

**判断**：网关→kimi 链路健康，是**生成阶段慢于客户端耐心**（8.9s 平均）。一天数据不足以下结论；
若明日仍 >20%，把 kimi 的并发上限调低或移出高峰时段的 primary。

## 8. 收口对照（11-08 审计 → 本轮复核）

| 11-08 审计项 | 本轮证据 | 状态 |
|--------------|----------|------|
| §3 cost 表缺失（`no cost entry` 2,627 条） | 全部落在 ≤11-08；11-09 起 **0 条** | ✅ 已收口 |
| §6/§12 body 哨兵 `auto` + 400 风暴 | 11-07: 162 次、11-08: 956 次 HTTPStatus 4xx；**11-09 起 ≤2/天** | ✅ 已收口（0.3.20/0.3.21 schema 修复链） |
| §1 截断口径 | 真截断率 0.11%（52/48.6k），45 例为 opencode 源站 ReadTimedout | ✅ 口径保持 |
| `moonshot flavored` 400 | 全窗口日志 **0 条**；0.3.21 投影为预防层（README `0169fbf` 已记实测） | ✅ 保持 |
| §12.4 volc 熔断 | 本轮确认是真配额（quota_backoff 四实例在列），非误熔断 | 观察至配额恢复 |
| §12.7 告警 webhook | 本轮未复核（遗留待办②，需用户提供 URL） | ⏸ 不变 |

其他长尾（`maxllm.err` 1,045 行全是部署时的 `GLIBC_2.38 not found`，
Debian 12 运行 Ubuntu 构建产物的历史残留；`ada`/`one_api_*` 为 11-08 前旧配置时代的
provider 名，当前 nas.toml 已无定义，日志中的 74 次 ada DNS 502 不会再发生）。

## 9. 数字速览（10 天窗口）

- 请求量：133,383 完成 / 10 天；高峰日 26.9k（11-08），常态 15~25k/天。
- 成本（日志侧）：minimax $447 + xiaomi $370 + volc 系 $171 + zhipu $8 + opencode $41 ≈ **$1,037/10 天**。
- 缓存命中率：主力 cloud provider 94.8%~98.0%；本地 groot 系 23~31%（短请求，正常）。
- 上游真截断：2,077 条 ERROR 中 2,057 条在 11-07~09 本地节点事故窗；当前窗口 0.11%。

## 10. 巡检记录（每 30 分钟一轮）

| 轮次 | 时间 (CST) | 窗口增量 | 结论 |
|------|-----------|----------|------|
| 1 | 2026-11-14 17:12 | 全量基线（11-04→14） | 本报告；kimi abort、minimax `?`-类重写率列入下轮复核 |
| 2 | 2026-11-14 17:23 | 增量 481 行（09:12:31Z→09:23:35Z） | 新增 §11：db-locked 丢账本写入，WAL 修复已合入；kimi 23.1% 稳定；minimax 改写量正常 |

## 11. 巡检第 2 轮新增：SQLite 写锁丢弃账本写入（已修复代码，待发布）

**现象**：`database is locked` 全窗口 38 次，其中 **31 次在今天（11-14）**，四种写路径全部命中：

| 写路径 | 今日丢写次数 |
|--------|-------------:|
| `Failed to persist session binding` | 19 |
| `Failed to record request log`（请求账本） | 10 |
| `Failed to persist circuit breaker state` | 5 |
| `failed to persist the provider routing evidence` | 3 |

爆发模式成簇：04:10:48→04:52 三次 5 秒间隔的连发（同一时刻三条不同表全部失败）、
07:12/07:17/07:29/07:44 同型、08:04 三连发。busy_timeout=5000ms 已设但不够 ——
存储用的是**默认 rollback-journal 模式**（NAS 上 `maxllm.db` 无 `-wal/-shm` 文件），
读写互斥、写者之间也互斥，高峰期偶发写等待 >5s 即静默丢行。

**修复**（本轮落地，commit 见 git log）：
`crates/maxllm-admin/src/sqlite_store.rs::set_pragmas` 追加
`PRAGMA journal_mode = WAL` —— 读写不再互斥、写者仍由 busy_timeout 兜底。
`cargo test --workspace` 全绿（含 285 条 gateway 集成测试）。
journal_mode 对既有 db 文件持久生效，只需下次发布部署后重启一次；重启后
`ls /home/oliveagle/opt/maxllm/maxllm.db-wal` 应出现文件、`database is locked` 应归零。
已丢的行无法找回（WARN 即丢弃），但丢的 10 行请求日志对 spend 汇总影响 <$0.05 量级。

**第 2 轮其余复核**（09:12:31Z→09:23:35Z 增量，481 行 / 404 完成）：
- kimi abort 率 23.1%（292/1,263，p50 5.8s）—— 与基线 24.7% 一致，继续观察；
- minimax `does not carry model` 改写 97 次（deepseek-v4.1-flash→MiniMax-M3），
  另 2×500 / 1×401 各触发一次 failover —— 量级正常，无异常；
- quota probe 最近 30 分钟 102 次全绿，今日仅 15:10/15:20 两次 429；
- volc 四实例 quota_backoff 未解除（streak=3 不变）。

## 附录 A：复现命令

```bash
# 1. 拉取 NAS 日志与 recordings
ssh oliveagle@oleNas01 'cat /home/oliveagle/opt/maxllm/logs/maxllm.log' > maxllm.log
ssh oliveagle@oleNas01 'cd /home/oliveagle/opt/maxllm/recordings && tar cz 2026-11-1[234]*.jsonl' | tar xz -C rec/

# 2. ERROR 分类（去重后）
rg " ERROR " master.log | sed 's/.* ERROR [a-z_:]*: //' | sort | uniq -c | sort -rn

# 3. Downstream/Upstream 按 sni 归因（sni→provider 映射见 nas.toml base_url）
rg "Downstream ConnectionClosed" master.log | rg -o "sni: [A-Za-z0-9.-]+" | sort | uniq -c

# 4. 流式结果三分（200 里谁是客户端断开 / 真截断）
python3 scripts/stream_outcomes.py --dir rec

# 5. 分 provider 时延/成本/缓存（近 3 天）
rg "^2026-11-1[234].*request completed " master.log | python3 <分 provider 聚合脚本>

# 6. breaker / quota / pool 状态
ssh oliveagle@oleNas01 'cat /home/oliveagle/opt/maxllm/logs/{pool-health-state.json,quota-probe-kimi.jsonl}'
```
