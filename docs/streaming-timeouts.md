# 流式请求的超时设计与阈值定法

面向后来维护者:为什么流式路径不能用总超时、当前三层活性保障各管什么、以及
**阈值该依据什么数据来定**。最后一节记录了三次定错阈值的过程——那部分比结论更值得读,
因为同类错误很容易再犯一次。

## 一、为什么流式不能只用总超时

`reqwest::ClientBuilder::timeout()` 是**总请求超时**,覆盖建连到 body 读完的全过程,
与流是否仍在推进无关。用它约束 SSE 流式响应会杀掉**成功**的请求:

实测一条 claude-opus-5 请求持续吐数据 712 秒、产出 12434 tokens / 700+KB,
在旧的 720 秒总超时下距硬顶仅 8 秒;撞上去的后果是客户端等了十分钟、输入 token
成本全部烧掉,最后拿到一个 timeout 错误。这是最贵的失败形态。

> **2026-08-04 更正**：这条 712s 请求后来被证实**不是健康的慢流**——那 700+KB
> 里大头是 208KB 的 Bedrock thinking 签名以大量小 `signature_delta` 帧下发,
> 每帧都重置 read_timeout,传输层看着健康,但客户端整整 712 秒没有一个可渲染的块。
> 「流在推进」和「客户端有东西可看」是两件事,本文的三层活性保障全部只测前者。
> 换 read_timeout 的结论本身仍然成立,但当时把它当「成功请求」是误判。
> 根因(签名透传)已在 0.9.5 修复,事件语义层观测(`first_render_ms`)已在 0.9.6
> 补上,全案见 docs/thinking-pipeline.md。

`read_timeout()` 才是流式该用的:它按**每次读取**计时,每收到一个数据帧即重置
(reqwest 0.12 `async_impl/body.rs` 的 `ReadTimeoutBody::poll_frame`,注释写着
`// a ready frame means timeout is reset`)。

非流式调用方(认证、token 刷新、版本探测、代理探活等,`build_client` 的 15 处调用)
**继续用总超时**——对一问一答那才是正确语义,不要顺手一起改。

## 二、当前三层活性保障

流式 client 由 `build_streaming_client()` 构建(`src/http_client.rs`),
两个阈值经 `config.json` 注入,默认值见 `DEFAULT_STREAM_*_TIMEOUT_SECS`:

| 机制 | 生产取值 | 计时对象 | 管哪种故障 |
| --- | --- | --- | --- |
| h2 keep-alive PING | 25s 间隔 / 15s ACK 超时 | 传输层往返 | 连接真死(网络断、对端进程没了),约 40s 识别 |
| `read_timeout`(空闲) | `streamIdleTimeoutSecs` = **300**(代码默认 90) | 上游**数据帧**间隔,每帧重置 | 连接活着但上游不再吐数据 |
| `timeout`(总) | `streamTotalTimeoutSecs` = **1800**(同默认) | 请求绝对寿命 | 上游以极低速率持续吐帧的 runaway |

> 空闲超时的**代码默认值仍是 90s**(`DEFAULT_STREAM_IDLE_TIMEOUT_SECS`),而本机生产在
> `config.json` 里覆盖为 300s。差异是刻意的:90s 已实证会误杀(见第四节第 2 条),
> 但改默认值需要重新构建镜像,而配置覆盖立即生效。**新部署若不显式配置,会拿到会误杀的
> 90s**——要么在 `config.json` 里设 300,要么把默认值改掉。

两者叠加(reqwest 原生支持,见 `async_impl/body.rs::response()` 的
`(Some(total), Some(read))` 分支——它把 read 包一层再套 total),不必二选一。

**三个容易搞错的边界:**

1. **h2 PING 不会重置空闲超时。** PING 是连接级控制帧,由 h2 层内部消化,不会作为
   body frame 冒到 `ReadTimeoutBody` 那一层。这是好事——否则 PING 每 25s 往返一次,
   空闲超时永不触发,整个机制形同虚设。
2. **下游 SSE ping(`PING_INTERVAL_SECS` = 25s)与上游超时无关。** 上游卡住时我们照常
   给客户端发 ping,所以客户端不会超时;但这也意味着**客户端不会自己放弃**,请求生死
   完全由上面两个超时决定。这正是总超时必须存在的理由。
3. **总超时不可省。** 只有空闲超时时,上游每 299 秒吐一帧就能让请求无限挂着,
   占用连接与凭据槽位。

## 三、阈值该依据什么数据

**依据是「健康长生成的最大静默」**,数据源是 `trace_phases.detail` 的 `max_idle_ms`
(0.8.6 起成功流也记录,见 `StreamPhaseGuard::max_idle_ms`)。查询:

```sql
SELECT MAX(CAST(substr(detail, instr(detail,'max_idle_ms=')+12) AS INT))
FROM trace_phases WHERE phase='streaming' AND detail LIKE '%max_idle_ms=%';
```

**但这是删失数据(censored data)**:静默超过阈值的健康流会被本地超时截断、
超过约 240s 的会被上游 RST,它们永远进不了成功分布。所以观测到的最大值**只是下界,
不是上限**,不能拿"没见过更大的"当安全依据。

**300s 的取舍**:上游对静默流有约 240s 的自有清理定时器,故真卡死通常会**先**被上游
RST 掉(错误串 `[decode]`,归因更准),我们这 300s 实际是兜底。刻意不去抢在上游前面——
90s 那次抢了,结果误杀(见下)。runaway 由 1800s 总超时兜住,空闲超时不必兼任该角色。

### 判断误杀 vs 真卡死:看产出量,不看静默时长

| | 真卡死 | 误杀(健康流被我们掐死) |
| --- | --- | --- |
| output_tokens | 1 ~ 228 | 数千(实例:6614) |
| 收到字节 | 125B ~ 71KB | 数百 KB(实例:380KB) |
| 静默后是否恢复 | 从不(5/5) | —— |
| 错误串标签 | `[decode]`(上游 RST) | `[timeout+decode]`(本地超时) |

只看静默时长会把两者混为一谈——这正是下面第二、三次错误的根源。

## 四、三次定错阈值的记录

留着是因为每次的错法不同,但根因相同:**拿手边现成的数字当依据,没先问这个数字测的是什么。**

1. **600s(0.8.6)→ 死代码。** 依据是"观测到一次合法静默 305.8s"。但那条请求只产出
   107 tokens、4.4KB,静默后从未恢复——**它是一具尸体**,305.8s 只是上游花了多久才发
   RST。用尸体的僵冷时间校准活人的体温。后果:上游最迟 305.8s 必先关,600s 永不触发。
2. **90s(0.8.7)→ 生产误杀。** 依据是"健康流最大静默仅 38ms/863ms,余量两个数量级"。
   但那两个数字取自被总超时砍掉的请求在**死亡瞬间**距上一 chunk 的间隔(`idle_ms`),
   不是整条流的最大静默(`max_idle_ms`,当时还没这个埋点)。这个取样系统性偏小——
   流更可能死在活跃期而非静默期。后果:上线 100 秒内即误杀一条已产出 6614 tokens、
   跑了 421 秒的健康流。
3. **编译期断言把上游行为写成不变量(0.8.7)→ 已删。** 曾写
   `assert!(IDLE >= 30 && IDLE < 240)`,上界的 240 是**上游的清理时限**。上游改行为
   后这个断言会静默失去意义,而外部系统的行为不该由本仓库的常量来断言。

**结论性的方法论**:定这类阈值前先回答三个问题——
这个数字测的到底是什么?样本是否被别的机制截断过(删失)?
两类现象的分布是否真的分开,还是我只观测到了其中一侧?

## 五、改阈值的操作

两个值都可配置,**改配置 + 重启即可,不必重建镜像**:

```bash
# data/config.json 加(或改)这两项,然后 podman restart kiro-rs
"streamIdleTimeoutSecs": 300,
"streamTotalTimeoutSecs": 1800
```

`serde(default)` 保证老配置(未设这两项)仍可启动,取代码默认值。改完验证:

```sql
-- 确认没有新的本地超时误杀:看是否有 [timeout] 且 output_tokens 较大的记录
SELECT substr(ts,6,14), output_tokens, duration_ms, substr(error_message,1,40)
FROM traces WHERE error_type='stream_interrupted' AND error_message LIKE '%timeout%'
  AND ts >= '<改动时间>' ORDER BY ts DESC;
```

output_tokens 大(数千)说明是误杀,阈值需调高;只有个位到几百说明是真卡死,属正常拦截。
