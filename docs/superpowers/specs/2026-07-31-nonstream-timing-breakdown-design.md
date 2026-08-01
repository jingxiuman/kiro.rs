# 非流式耗时分段与统一耗时分布条

- 日期：2026-07-31
- 分支：`feat/ops-module`
- 状态：已实现并验证；非流式四段未经真实上游请求验证（见「验证缺口」）
- 前置：[`2026-07-26-ops-stream-phase-topology-design.md`](./2026-07-26-ops-stream-phase-topology-design.md)
  —— 本设计取代其中「非流式不记 phases」一节

## 背景

「请求日志」页对非流式请求只有一个总耗时。`firstTokenMs` 永远 `null`，`phases` 永远空数组，
前端渲染一行「非流式请求，无流生命周期分段」。

线上真实数据里存在 15.6s、38.9s 的非流式请求，面板上对它们只有一个数字，看不出慢在哪。

根因是**当时的实现拆不开**：整段生成塌在 `handlers.rs` 一次 `response.bytes().await` 里，
上游从收到请求到吐完最后一个字节，全部计入这一个 await。

前置 spec 由此得出「非流式无流生命周期，不造空壳段」。**这一步推理过头了**：
「当前实现拆不开」不等于「本质上无可分段」。改成逐 chunk 读之后，
「等上游吐第一口」与「收完剩余 body」立刻成为两段可分的时间——而这两段的运维含义
完全不同（前者是上游 prefill/排队，不可优化；后者是传输，与出口链路相关）。

## 目标

1. 非流式给出与流式同口径的分段耗时，能区分「等上游想」与「收得慢」。
2. 重试的 backoff 退避时间在时间轴上可见。
3. 一个可视化同时服务两条路径，不为非流式单开一套渲染。

### 非目标

- **不改响应内容与状态码。** 逐 chunk 读只改读法，累积完仍一次性 `feed` 给 decoder，
  字节序列与原先完全一致。
- **不改超时语义。** 两条路径共用同一个 client，不额外包一层超时。
- **不做跨进程分段。** 与前置 spec 的非目标一致。
- **不追求严格实测的每跳起点。** `started_ms` 是回推估算值，见「已知精度限制」。

## 设计

### 段名：`first_token` 两条路径共用，其后分开

| 路径 | 段序列 |
|---|---|
| 流式（实时 / 缓冲） | `first_token` → `streaming` → `finish` |
| 非流式 | `first_token` → `body_read` → `decode` → `assemble` |

`first_token` **刻意共用**：两条路径下它的语义完全相同——「建连成功到上游吐出第一口数据」。
共用带来两个直接好处：`ops.rs` 的 `phase_baseline`（按 `phase × proxy_url` 聚合失败率）
自动覆盖非流式，前端 `PHASE_LABEL` 也不必分支。

其后不共用，因为工作内容不同：流式是边收边发，非流式是收全 → 解码 → 组装。
硬凑成同名会让 `phase_baseline` 把两类不同的工作混进同一个失败率里。

### 逐 chunk 读：`read_non_stream_body`

```
open(first_token)
  ├─ 首个非空 chunk → mark_first_token / close(first_token) / open(body_read)
  ├─ 后续 chunk     → 累积到 Vec
  ├─ Err            → close(当前段, stream_interrupted, 已收字节)
  └─ 干净 EOF       → close(body_read, success, 总字节)
                      或（一个字节都没来）close(first_token, upstream_truncated)
累积完 → 一次性 decoder.feed(&buf)
```

三个关键点，都是「不这样做会怎样」：

**空 data frame 不算首字节。** h2 允许空 data frame。若把它当首字节，`first_token_ms`
会偏早，且之后读失败会被误归因到 `body_read`（传输问题）而非 `first_token`（上游没响应）。

**空 body 必须显式关段。** 上游返回 2xx 但 body 为空时，`first_token` 段永远等不到 chunk。
不显式关掉的话它会一直挂在 `open_phase` 里，最终被 `finalize` 静默丢弃——该请求在日志里
一段分段都没有，等于改动白做。记为 `upstream_truncated` 而非 `success`：空响应体是上游问题。

**错误归因看 `first_seen`。** 断在首字节前后，责任方不同，两段各自的 `outcome` 都要落库。

### 超时语义为何不变

两条路径共用 `provider.rs` 的 `client_for` → `build_streaming_client`，该 client 设的是
`read_timeout`（空闲超时）而非总超时。空闲超时按「单次读之间的间隔」计算，
逐 chunk 读与一把梭读受同一个约束，不存在「拆细了就更容易超时」。

若该 client 改回总超时，本设计需重新评估——总超时下逐 chunk 与一次性读仍等价，
但那时 `body_read` 段的长尾会直接撞总超时，含义与现在不同。

### `trace_attempts.started_ms`：为什么在 sink 侧回推

新增可空列，记每跳相对请求起点的偏移。

**没有它的后果**：attempt 只有 `duration_ms`，前端只能把各跳首尾相接顺序堆叠，
于是重试之间的 backoff 退避被完全抹掉——一条「跑了 12s、其中 500ms 在等退避」的链路
看起来和「连续跑了 11.5s」一模一样。

**为什么不透传请求起点进 provider**：provider 的 `call_api` 不持有 `RequestTracer` 的
`started_at`，要拿到就得改 `emit_attempt` 及其上游一串签名，把一个纯观测量渗进调用链。
`RequestTracer::on_attempt` 已经在 sink 侧、且持有 `started_at`，在那里回推：

```rust
let elapsed = self.started_at.elapsed().as_millis() as u64;
attempt.started_ms = Some(elapsed.saturating_sub(attempt.duration_ms));
```

provider 侧显式填 `None`，语义是「我算不出」，而不是 0。

### 已知精度限制

回推值实际等于：

```
真实起点偏移 + duration 的毫秒截断误差 + provider 采样 duration 到 on_attempt 取 elapsed 之间的延迟
```

同步紧邻调用下误差约 1ms。但若线程在构造 `TraceAttempt` 后、进入 `on_attempt` 前被长时间
抢占，该延迟会整体加到 `started_ms` 上，使该跳在时间轴上右移，可能制造出不存在的前置空隙。

**这是估算值，不是实测起点。** 前端因此不把毫秒级空隙一律断言为 backoff——
条上把「两跳之间的洞」标为「重试等待」，其余位置的洞标为「未归因」。

要严格实测需把请求起点透传进 provider、在 attempt 真正开始时取偏移。本次不做：
代价是渗签名，收益只是把 ~1ms 误差消掉，而这条时间轴的用途是看百毫秒级的分布。

### 历史行不回填

`started_ms` 对该列存在前的行为 `NULL`。前端 `a.startedMs ?? cursor` 走顺序堆叠，
并显式标注「该记录早于每跳起点埋点，跳的位置由耗时顺序推算」。

理由与前置 spec 的 `proxy_url` 一致：真实起点无法追溯，回填等于编造，
而「看起来确定的值」正是前一次误判的成因。

### `AVG(first_token_ms)` 钉死流式——请勿"顺手"去掉

`ops.rs` 的 `overview` 把该指标显式限定为流式：

```sql
CAST(AVG(CASE WHEN is_stream = 1 THEN first_token_ms END) AS INTEGER)
```

改动前非流式该列恒为 `NULL`，SQLite 的 `AVG` 忽略 `NULL`，所以这个指标**事实上一直只含流式**。
非流式开始有值后，若不加这个 `CASE`，指标口径会从「流式首 token」悄悄变成
「流式+非流式混合平均」——与历史窗口不可比，且会随两类流量占比漂移，
看起来像"首 token 变慢了"，实际只是非流式占比上升。

两者的运维含义也不同：流式的首 token 意味着「此后可以边收边发给客户端」；
非流式的首字节之后还要收完整段才能响应。

要观测非流式首字节，应新增独立指标，不要改这个指标的定义。

### `interrupted_after_bytes` 语义分叉——已知的技术债

该字段在两条路径下含义相反：

| 路径 | 含义 |
|---|---|
| 流式 | 已**下发给客户端**的字节 |
| 非流式 | 从**上游收到**多少就断了（非流式在组装完成前不向客户端写任何字节） |

前端按 `isStream` 分文案，现有页面不会误导。

**更干净的模型是拆成两个可空字段**（`downstream_sent_bytes` / `upstream_received_bytes`）。
本次未拆：该字段今天只有 UI 一个消费者，为尚未出现的导出/告警消费者做加列迁移，
不如等真有需求时再拆。

**若将来新增导出、聚合或告警消费该字段，必须先拆。** 只按字段名统计会把
「非流式从上游收到的量」当成「已发给客户端的量」。

## UI：耗时分布条

`admin-ui/src/components/trace-timing-bar.tsx`，挂在展开详情顶部，流式非流式共用。

两层拼接成一条时间线：attempt（取凭据→响应头，N 跳含重试与 backoff 空隙）
+ phase（响应头之后的处理）。

配色按**「时间花在哪类事情上」**分，而非按段名逐个配——扫一眼就知道瓶颈归谁：

| 颜色 | 含义 | 段 |
|---|---|---|
| 琥珀 | 等上游 | `first_token` |
| 绿 | 传数据 | `streaming` / `body_read` |
| 紫 | 本地解码 | `decode` |
| 灰蓝 | 本地收尾 | `finish` / `assemble` |
| 天蓝 / 玫红 | 每跳成功 / 失败 | attempt |
| 灰斜纹 | 重试退避 | 两跳之间的洞 |
| 淡灰 | 未归因 | 其余洞与尾隙 |

### 核心不变式：宽度之和恒等于总时长

**这是这个组件唯一真正的正确性要求**，也是本次两处返工的根源。

段来自两套独立埋点（attempt 在 provider 侧计时，phase 在 handler 侧计时），
毫秒截断与边界重合会让两段轻微交叠。因此不能直接按 `durationMs` 排 flex：
宽度之和会超过 100%，flex 等比压扁所有段，长段占比就被读错——而占比是这条图唯一
要传达的信息。

`layout()` 用一遍游标解决：

- 交叠 → 钳位可见宽度（`widthMs`），但保留实测 `durationMs` 供 tooltip
- 空洞 → 显式补段。两跳之间标「重试等待」，其余标「未归因」
- 0ms 段与被完全吞掉的段 → **`widthMs` 记 0 但保留**，不丢弃
- 尾隙 → 只要 `cursor < total` 就补，不设阈值

`barWidths()` 再做**双向**配平：极短段垫到 `MIN_VISIBLE_PCT`（0.5%）以保证可见可 hover，
垫出来的量从「宽到垫得起」的段按可扣余量比例扣回；总和不足 100% 时补给最宽段。

### 为什么"不丢弃 0ms 段"要单独写下来

初版 `layout()` 对 `end <= cursor` 直接 `return`，丢弃该段。后果：

- 0ms 的 `finish` 段从条上**和图例里**一起消失
- 完全落进前一跳区间的**失败段**（如 attempt `[0,100)` + 失败 phase `[99,100)`）也消失

一个失败段被静默吞掉，是这个组件最坏的失效形态——它的存在目的就是暴露这些段。

同类问题在 `barWidths` 上也发生过一次：垫高最小宽度后总和 1005px > 轨道 992px，
`overflow-hidden` 从尾部裁掉，末尾的「未归因」段在条上渲染成 0px，而图例里还列着它。

**共同教训**：一个以「暴露信息」为目的的组件，任何「信息没显示出来」都不可能是正常的。

## 测试

Rust 侧（`handlers.rs` 的 `tracer_tests`、`trace_db.rs` 的 `tests`）：

| 测试 | 锁住什么 |
|---|---|
| `non_stream_body_read_splits_first_token_from_body` | 四段中前两段成立，且累积字节与原始一致 |
| `empty_upstream_body_still_closes_first_token_phase` | 空 body 不漏关段（否则该请求无任何分段） |
| `empty_leading_chunk_does_not_count_as_first_token` | 空 data frame 不算首字节 |
| `empty_chunk_followed_by_data_marks_first_token_on_the_data` | 空 chunk 后来数据，首字节记在数据那一刻 |
| `on_attempt_backfills_started_ms_preserving_gap_between_hops` | 两跳起点**数值**与之间的空隙 |
| `attempt_started_ms_round_trips` | 新列往返，且 backoff 空隙可识别 |
| `migrate_adds_attempt_started_ms_and_keeps_old_rows_readable` | 老库加列幂等，历史行为 `NULL` 而非 0 |

倒数第三条值得说明：初版只断言 `started_ms.is_some()`，**全部错算成 0 也能通过**——
而"全 0"恰好等于"色块条上重试 backoff 全部消失"这个失败形态。断言必须落到数值。

前端 `layout` / `barWidths` 无 JS 测试框架，以一次性脚本核过 16 例算术，
含 0ms 段、被吞失败段、1ms 尾隙、2ms 极短总时长、全 0ms、老库无 `started_ms`
等边界，全部满足「宽度之和 == 总时长」。**该脚本未入库**——若后续再改这两个函数，
建议补成常驻测试。

## 验证记录

- `cargo test` 773 passed（含上述 7 条）
- `cargo clippy --all-targets` 本次改动文件零新增告警
- `npm run build`（含 `tsc -b`）通过
- 老库迁移：以线上 `traces.db` 的 SQLite backup 副本（8920 条 trace、1870 条非流式）
  实启动，加列成功、8801 条历史 attempt 全为 `NULL`
- 浏览器实测：色块条宽度之和 992.1px / 轨道 992px（亚像素），最小段 4.95px 可见可 hover；
  0ms `finish` 段与被吞失败段修复后均回到条上和图例

### 验证缺口

**非流式四段未经真实上游请求验证。** 本机无可用凭据，四段的验证依赖单测 + 合成数据
（直接向测试库插入 trace 行）。写入路径本身有单测覆盖，但首次真实流量确认仍待上线后观察。

上线后建议看头几个非流式请求：`首Token` 列应不再是 `—`，展开后应有四段。

## 外部评审

经 `codex exec` 评审（无阻断项，3 应修 + 3 建议）。已修 4 项：

1. `decoder.feed` 出错时只 warn，`close_phase(DECODE)` 只看 `tool_json_error`，
   导致确凿的帧解码失败被记成 `success`——**新加的统计在污染自己的基线**。
   现在错误留下来，且优先级钉死：帧解码失败 > tool json 失败（前者说明字节流没读全，
   后者只是内容不合法，先报靠底层的才不指错归因方向）。
2. 0ms / 被吞段被丢弃（见上）。
3. 尾隙阈值导致条形和 < 100%，且注释谎称"已配平到 100%"（实际只单向）。
   顺带纠正一处假注释：`totalSlack <= 0` 分支原写"交给 flex 收缩"，
   实际 `flexShrink: 0` 不会收缩。
4. 空 data frame 被当首字节。

未采纳 1 项：拆 `interrupted_after_bytes`（理由见上）。

**第 2 项是我在浏览器实测时看见过并判为"一致"放过的。** 当时观察到流式那条
`segCount=5` 而非 6，0ms 的 `finish` 段不在其中，我给出"一致"这个结论却说不出机制。
说不出机制的"符合预期"是事后合理化，不是验证。

## 遗留问题

**`max_idle_ms` 仍未持久化。** `StreamPhaseGuard`（`handlers.rs`）对每条流都算出了最大
chunk 间隔，但只进日志行与失败时的 `detail`，成功流的值被丢弃。它是校准
`STREAM_IDLE_TIMEOUT_SECS` 的直接依据（该值本轮走过 600s → 90s → 300s），
落库后可直接用分位数定阈值，不必靠翻日志。本次未做：与「分布可视化」不同目标。

**`RequestTracer::new` 的位置未动。** 它构造于请求转换、token 估算、缓存计量**之后**，
因此 `duration_ms` 不含这些准备工作，条上表现为开头的「未归因」段。往前挪能让总耗时
更诚实，但会改变 `avg_duration_ms` 的口径、破坏历史可比——需单独决策。

**非流式的 `decode` / `assemble` 段目前恒定很短**（实测 260ms / 25ms 量级，
相对总时长可忽略）。若长期观察确认它们始终无信息量，可考虑合并以简化条形；
但在拿到足够真实流量前不动——现在合并等于用推测替换观测。

## 已排除项

- **不给非流式补 `streaming` 段。** 非流式在组装完成前不向客户端写字节，
  没有"边收边发"这一阶段，造一个同名段会让 `phase_baseline` 把两类工作混为一谈。
- **不在列表行内加迷你色块条。** 需重排行高与列宽，收益是"扫一眼列表就看出瓶颈"，
  但当前展开一行的成本已经很低。待有"批量筛查慢请求"的实际需求时再做。
- **不改非流式 tool-json 错误的 502 行为。** 本次只让 `DECODE` 段的 `outcome` 如实反映，
  响应语义不动。
