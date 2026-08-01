# 运维模块：流式分段埋点与错误详情拓扑图

- 日期：2026-07-26
- 分支：`feat/ops-module`
- 状态：已实现；其中「非流式不记 phases」一节已于 2026-07-31 被取代
  （见 [`2026-07-31-nonstream-timing-breakdown-design.md`](./2026-07-31-nonstream-timing-breakdown-design.md)）

## 背景

排查一次客户端报错时暴露了观测盲区。错误原文：

```
Upstream ended before completing tool_use toolu_bdrk_01F148TfZJ7eKfEc9P34D7ym (str_replace)
JSON input; buffered 331 bytes. The tool call was not forwarded to the client.
```

该文案由 kiro-rs 自身产生（`src/anthropic/stream.rs:945`，`ToolJsonAccumulatorError::IncompleteJson`）。
机制：Kiro 把 tool_use 的 `input` JSON 拆成多个 `toolUseEvent` 分片下发，最后一片带 `stop=true`。
kiro-rs 按 `tool_use_id` 累积，只在 `stop=true` 时整体解析。流在此之前结束即报此错。

**这是护栏正常触发，不是 bug**：它拒绝把半截 JSON 当成完整工具调用交给客户端执行。

### 排查中撞到的两个盲区

**盲区一：attempt 层对流中截断是盲的。**
历史上 9 次同类错误，`trace_attempts` 记录全部是 `outcome=success / http_status=200`。
原因是 attempt 只覆盖到「响应头拿到」，body 中途断裂发生在其后，无任何埋点。

按现有数据画拓扑图，会得到「每跳全绿 + 最终态红」的图，比没有图更误导。

**盲区二：`proxy_url` 的 `NULL` 语义双关。**
`src/admin/trace_db.rs:301` 注释写作「历史行保持 NULL = 未知/直连」，把两种含义合并。
前端 `admin-ui/src/components/trace-log-page.tsx:171` 硬编码 `{a.proxyUrl ?? '直连'}`，
把「未知」直接渲染成「直连」。

排查中据此得出「9 次截断全部走直连」的结论，属误判。实际情况：

- 该列首次写入时间为 `2026-07-27T05:01:21`（UTC，ops-module 镜像重启时刻），此前所有行为 NULL
- 重启后 455 跳中 NULL 计数为 0
- 9 个凭据全部自带 `proxyUrl`（8 个 `socks5://…:10401`、1 个 `socks5h://…:10301`），
  `effective_proxy`（`src/kiro/model/credentials.rs:392`）仅在凭据未配置时回落全局，
  故 `direct` 分支实际不可达，全局 `http://…:10200` 从未被使用

## 目标

在错误详情中定位「流在哪一段断的」，并给出同维度历史对照，使单条错误可归因。

### 非目标

- **不做跨进程拓扑。** nginx(9092) → sub2api(9812) 两跳在本进程之外，需 request-id 贯穿
  与日志采集，成本量级不同，另行决策。
- **不回答「代理是否为根因」。** 见「遗留问题」。

## 设计

### 架构

`RequestTracer`（`src/anthropic/handlers.rs:124`）已是「进程内累积、`finalize` 时一次性落库」形态。
分段埋点沿用同一模式，不新开写入路径：

```
RequestTracer
├── attempts: Mutex<Vec<TraceAttempt>>   （已有）→ connect / headers 两段
└── phases:   Mutex<Vec<TracePhase>>     （新增）→ first_token / streaming / finish 三段
                                 ↓
                    finalize() 同一事务写入
```

职责边界：

| 段 | 数据来源 | 说明 |
|---|---|---|
| connect / headers | `trace_attempts`（已有） | provider 层每跳已记，含重试 |
| first_token / streaming / finish | `trace_phases`（新增） | 仅针对最终成功建连后的流生命周期 |

前端拓扑图由两张表拼接渲染，后端不做合并。attempts 是 N 跳（含重试），phases 是 1 条流，
基数不同，合成单表会产生重复行。

### 数据模型

```sql
CREATE TABLE IF NOT EXISTS trace_phases (
    trace_id    TEXT NOT NULL,
    seq         INTEGER NOT NULL,      -- 段序号，保证渲染顺序
    phase       TEXT NOT NULL,         -- first_token | streaming | finish
                                       -- （2026-07-31 起非流式另有 body_read | decode | assemble）
    started_ms  INTEGER NOT NULL,      -- 相对请求起点的偏移
    duration_ms INTEGER NOT NULL,
    outcome     TEXT NOT NULL,         -- 复用 outcome 常量
    bytes       INTEGER,               -- 该段累计下发字节
    detail      TEXT,                  -- 错误片段 / 判别位摘要
    PRIMARY KEY (trace_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_phases_phase_outcome ON trace_phases(phase, outcome);
```

索引服务于对照基线查询：`JOIN trace_attempts USING(trace_id) GROUP BY phase, proxy_url`。

容量：约 1200 请求/天 × 3 段 = 3.6k 行/天，7 天保留约 25k 行。清理复用现有 retention 任务。

`outcome` 复用 `src/admin/trace_db.rs:136` 的 `mod outcome`，其中
`UPSTREAM_TRUNCATED` / `UPSTREAM_INVALID` 已存在且已定义「计入 / 不计入代理健康」的区分。

### streaming 段判别位

区分「上游主动掐流」与「客户端断开」。两者在 `body_stream.next()` 处均表现为 `Some(Err(e))`，
但归因相反。三个判别位写入 `detail`：

| 判别位 | 来源 | 含义 |
|---|---|---|
| `client_gone` | 发送端关闭 / body 被 drop | true = 客户端断开，不罚代理 |
| `bytes` | 已有的 `sent_bytes` 累加器 | 断前已下发字节数 |
| `idle_ms` | 距上一 chunk 的间隔 | 区分「突然断」与「先卡死再断」 |

三者均可从 `handlers.rs:771` 的 `stream::unfold` 状态元组取得，不引入新依赖。

### proxy_url 语义修复

```
真直连  → 写入字面量 "direct"
NULL    → 仅保留给该列存在前的历史行 = 未知
```

前端三态渲染：`"direct"` → 直连，`null` → 未知（灰色），其余 → 出口 URL。

**历史行不回填。** 其真实出口无法追溯，回填等于编造，且本次误判正源于「看起来确定的值」。

### 数据流

| 路径 | 位置 | 是否记 phases |
|---|---|---|
| 实时流 | `handlers.rs:771` 的 `stream::unfold` | 全三段 |
| 缓冲流 | `create_buffered_sse_stream`（`handlers.rs:1586`） | 全三段 |
| 非流式 | `handlers.rs:985` 一次性 `feed` | 否，仅 attempts |

非流式无流生命周期，不造空壳段；前端对其只渲染 attempts 层。

> **⚠️ 此判断已于 2026-07-31 被取代。** 非流式现在记四段
> （`first_token` / `body_read` / `decode` / `assemble`），前端与流式共用同一套渲染。
>
> 当初的前提是「非流式只有一次性 `bytes()` 读取，整段生成塌在一个 await 里，拆不开」——
> 前提本身没错，但它是可以改的：改成逐 chunk 读之后，「等上游吐第一口」与「收完剩余
> body」就成了两段可分的时间。当时把「当前实现拆不开」当成了「本质上无可分段」。
>
> 见 [`2026-07-31-nonstream-timing-breakdown-design.md`](./2026-07-31-nonstream-timing-breakdown-design.md)。

实时流三个标记点均落在现有分支内，不新增控制流：

```
Some(Ok(chunk)) 首次   → mark_phase(first_token)        // 现 mark_first_token 处
Some(Ok(chunk)) 后续   → 累加 bytes / 刷新 idle_ms       // 现 sent_bytes 处
Some(Err(e))          → close_phase(streaming, 判别位)   // 现 report_stream_outcome 处
None                  → close_phase(finish, 累积器结果)   // 现 tool_json_error_message 处
```

`None` 分支中 `generate_final_events()` 已调用 `tool_json_accumulator.finish()`，
`IncompleteJson` 在此产生，映射为 `finish` 段 `outcome = upstream_truncated`。

### 错误处理

- 埋点失败不影响主流程。沿用 `finalize` 开头 `let Some(store) = &self.store else { return }`
  的 no-op 约定；phases 与 attempts 同事务，失败一起丢，不做补偿。
- `client_gone` 判定取保守方向：仅在明确检测到发送端关闭时判客户端断开，其余归上游断。
  误判代价不对称——判成「客户端断开」会漏罚真实故障；判成「上游断」最多多几次失败计数，
  距 5 次自动禁用阈值仍有缓冲。宁可冤枉，不可漏放。
- 表迁移用 `CREATE TABLE IF NOT EXISTS`，与 `trace_db.rs:301` 现有 ALTER 模式一致。

### 测试

按 TDD，先写失败测试。模板参照 `trace_db.rs:979` 的 `attempt_proxy_url_roundtrip`。

1. `phases_roundtrip` — 三段写入后读回，顺序与字段一致
2. `phase_baseline_aggregation` — 两出口 × 成功/中断样本，断言分组失败率正确
3. `truncated_tool_json_marks_finish_phase` — 核心回归。构造 `stop=true` 前结束的
   toolUseEvent 流，断言 `finish` 段 `outcome == upstream_truncated`
4. `client_disconnect_not_charged_to_proxy` — 客户端断开时 `report_proxy_failure` 不被调用
5. `proxy_url_tri_state` — `"direct"` / `null` / URL 三态不互相塌陷

第 3、5 条分别锁住本次事故的两个成因：故障不可见、数据在 UI 上失真。

### UI

错误详情内新增横向泳道，两层对应两个数据源：

```
尝试链路  ├─#0 cred#3 socks5h://…10301 ─ 200 ✓ 312ms ─┐
          └─#1 …（无重试时仅一行）                      │
                                                       ↓
流生命周期 ├ first_token ✓ 1.2s ├ streaming ✓ 18.4s/20211B ├ finish ✗ upstream_truncated
                                        ↑ 近24h 同出口该段中断率 x.x% (n/N)
```

（以上数值为布局示意，非实测。）

每段下方小字为对照基线，回答「本段失败是特例还是该出口常态」。

## 遗留问题

**「代理是否为流中断根因」仍未定论，本设计不解决。**

当前 455 跳中 453 跳走同一出口 `socks5h://192.168.110.56:10301`，代理与上游的贡献在观测上
完全共线。这不是样本量问题，是实验设计问题——没有对照组，再多埋点也分不开。

定论需人为造对照：将某凭据 `proxyUrl` 设为 `"direct"` 或换出口，同一超长 prompt 各跑 N 次，
比对 `IncompleteJson` 发生率。该动作会影响线上，需单独决策，不在本设计范围。

本设计交付的是「下次能看见断在哪一段、以及该段是否异常」。

## 已排除项

- **`ops_events` 空表不是缺陷。** 其 category 仅三种（`ops.rs:33-37`）：`proxy_auto_disable` /
  `proxy_reassign` / `proxy_probe_disable`，均为处置动作而非错误发生。单次流中断只累加失败
  计数，攒够 5 次才禁用才记事件。0 行即从未触发自动禁用，符合设计。
- 运维页「最近上游错误」面板（`ops-page.tsx:385`）复用 traces API，不读 `ops_events`，不受影响。
