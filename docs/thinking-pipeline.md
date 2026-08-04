# Thinking 管线：五个缺陷的修复记录与设计原则（0.9.4–0.9.8）

面向后来维护者。2026-08-03~04 一天内定位并修复了 thinking 转译链路的五个问题，
全部源于同一份证据链：一条 7-31 的病态 Claude Code 会话（712s 首块延迟、2.9MB
请求体、22 分钟零 chunk 失败）+ 一条原生 Anthropic 通道的对照会话。本文记录
根因、修法、验证证据，以及**为什么这些缺陷长期没被发现**——那部分比修法更值得读。

对照基线（原生 Anthropic 通道，753 条消息实测）：thinking 永远先于 tool_use
（0/290 颠倒）；多段思考各自独立块、各带签名；思考正文不进客户端往返
（`display:"omitted"` + 签名恢复）；签名 0.4–8.4KB 密码学凭证。
kiro.rs 的五个缺陷全部是对这套语义的偏离。

## 缺陷一览

| # | 缺陷 | 版本 | 提交 |
| --- | --- | --- | --- |
| 1 | `thinking_extracted` 一次性闩锁：第二段思考连 `<thinking>` 标签漏进正文 | 0.9.4 | 3ea6708 |
| 2 | Bedrock 真签名（最大 208KB）透传给客户端并每轮回传 | 0.9.5 | 7968244 |
| 3 | trace 缺事件语义层观测（块顺序 / 首个可渲染帧不可见） | 0.9.6 | 6eb25c8 |
| 4 | 原始请求体无留存，「未知字段膨胀」类问题无法复盘 | 0.9.7 | 621af43 |
| 5 | 不解析 `display:"omitted"`：客户端每轮背 66K 字符思考正文往返 | 0.9.8 | 5e65c14 |
| - | thinking/tool_use 顺序颠倒（67%）——**随 #1 修复消失**，未单独改动 | 0.9.4 | 见下 |

## 1. 闩锁退役（0.9.4）

`stream.rs` 的 `thinking_extracted` 在第一段思考提取后置 true、**全文件无重置**，
后续 `<thinking>` 标签探测被短路，第二段思考整段走 text_delta。

修法：字段删除，状态机只看 `in_thinking_block`，每对标签各开一块——对齐原生
interleaved thinking 语义。连带修了两个同构问题：

- 开标签跨 chunk 探测从「盲扣 10 字节」收敛为「只扣确实是 `<thinking>` 前缀的尾巴」
  （`partial_thinking_start_tag_len`），普通文本立即下发；
- tool_use 是明确段落边界，`handle_tool_use` 时统一 flush thinking 缓冲与 invoke
  嗅探缓冲——半截标签不可能跨过结构化工具调用续接。

**顺序颠倒的消失**：7-31 报告测得 110/164（67%）的消息 tool_use 先于 thinking；
另一会话中混入的旧版流量复测同为 8/12（67%）——确定性缺陷。0.9.4 部署后
`stream_shape` 实测 37/37 零颠倒。机理：旧闩锁模式下 thinking 块经常来不及在
tool_use 前开出；闩锁退役后块按到达即开即闭。

## 2. 真签名不透传（0.9.5）

真签名的完整生命周期是纯浪费：下发给客户端 → 存进历史 → 每轮回传 →
`ContentBlock` 无 signature 字段被 serde 静默丢弃 → **从不回到上游**。
而 Bedrock 真签名可达 208KB（为 thinking 正文的 5-18 倍，实为加密的思考内容
而非密码学凭证），把病态请求体滚到 2.9MB（签名占 45%）。

修法：流式与非流式统一下发 26B 占位符。`ReasoningContentEvent.signature`
保留字段反映线格式但不再读。

**判断标准**：签名该不该透传，取决于**上游是否回收校验**。kiro 上游不回收 →
占位符正确；sub2api 的上游（原生 Anthropic/Vertex）回收 → 它必须透传真签名
（并有缺签名过滤/重试三层防护）。同一字段，两个项目的正确做法相反，不可互抄。

## 3. 流形态摘要（0.9.6）

「流在推进」≠「客户端有东西可看」。三层活性保障（h2 PING / read_timeout /
总超时）全部只测前者，712s 病态请求曾被误判为健康慢流
（见 streaming-timeouts.md 的更正注记）。

trace 新增两列（老库自动迁移）：

- `stream_shape`：`[{t:块类型, ms:出现时刻, b:内容字节}]`，只存形态不存内容；
- `first_render_ms`：首个可渲染帧（非空 thinking/text delta 或 tool_use 块开始），
  与 `first_token_ms`（首个上游 chunk）正交——两者差值大即「假活流」。

采集在事件转字节的出口侧（`RequestTracer::observe_events`，先于 finalize），
与客户端所见一致。健康流实测：`first_render_ms ≈ first_token_ms + 1ms`。

## 4. 请求体全量保留（0.9.7）

存**线上原始字节**而非 serde 解析后的视图——后者恰好丢掉「未知字段膨胀」
所在的字段（208KB 签名当初就藏在 `ContentBlock` 不认识的 signature 里）。

`storeRequestBodies: true` 时 gzip 按天落 `request_bodies/YYYY-MM-DD/<trace_id>.json.gz`，
与 trace 同 id 关联，保留期随 `traceRetentionDays`。
读回：`GET /api/admin/traces/{trace_id}/request-body`。
成本（实测 9.5K 请求/周）：压缩后约 40MB/天。默认关闭——内容含用户源码与对话。

## 5. omitted 轻量往返（0.9.8）

CC 2.1.220 起发 `thinking:{type:"adaptive",display:"omitted"}`。原生 API 尊重它：
思考正文不下发，客户端历史里是空文本+签名，回传时服务端凭签名恢复。
kiro.rs 此前不解析 display，全文照发——实测客户端每轮回传 77 块 66K 字符思考正文。

修法（签名即恢复凭证，三段）：

- 下行：omitted 时正文 delta 拦下（`emit_thinking_text`），正文 gzip 存
  `thinking_texts/`，签名=`kiro-thinking-v1:<id>`（`thinking_close_signature`）；
- 回程：handlers 预处理（`restore_omitted_thinking`）凭键回填历史正文，
  **先于** token 计数 / cache 计量 / 转换——上游所见与计量口径一致，
  推理上下文零损失（converter 会把历史 thinking 包进上游 assistant 历史）；
- 键过保留期：保持空文本不伪造——与原版签名过期语义一致。

原版靠加密签名做无状态恢复；kiro.rs 没有那把钥匙，用「服务端有状态存储+键」
等效实现。非 omitted 客户端行为零变化（有回归测试钉住）。

## 设计原则（0.9.5 与 0.9.8 是同一条原则的两次应用）

**只在往返里携带对端真正消费的东西。** 判断标准不是字段名，而是链路对端
消费不消费：客户端不展示的正文不下发（omitted），上游不回收的签名不透传。

**观测要落在用户感知的语义层。** 传输层指标（耗时/状态码/字节）回答不了
「客户端看到了什么、什么时候看到的」；`stream_shape`/`first_render_ms` 补的是这一层。

## 为什么拖了这么久才发现

1. **误判先例**：712s 病态请求曾被当作「成功的慢流」写进超时文档，
   结论方向反了（详见 streaming-timeouts.md 更正注记）；
2. **均值面板看不见长尾**：request-latency.md 的 n=3260 样本里几乎没有 2.9MB
   body 的极端情况；
3. **签名不计 token**：2.9MB body 里 45% 是签名，但 usage 面板一切正常——
   不可读 blob 只占带宽不占计量；
4. **服务端对事件语义盲**：块顺序、可渲染帧这类问题只在客户端 transcript 里可见，
   而 transcript 在用户机器上。

修复 3 与 4 针对的正是这四条盲区本身。

## 遗留观察点

- 块顺序：`stream_shape` 样本到数百条后复查一次（一条查询：kinds 里 tool_use
  的 index 早于 thinking 即颠倒），仍为 0 即可正式关闭；
- omitted 恢复失败率：客户端回传 `kiro-thinking-v1:` 键但 blob 已过期/丢失的
  频次（目前无计数，出现异常再补）；
- `request_bodies/` 磁盘水位：约 40MB/天，7 天滚动 ~300MB，异常增长值得看一眼；
- 历史遗留的 8 条旧占位符颠倒消息回传时原样透传，无碍。

## 数据来源

- `reports/2026-08-03-thinking-visibility/`（仓库外，kiro 部署目录）：7-31 病态
  会话的完整归因，含 turns.csv / thinking_blocks.csv / 复现脚本；
- 原生通道对照：4 条会话 753 条消息的块序/签名统计（本文基线数字）；
- 0.9.6+ 线上 `stream_shape`/`first_render_ms`：`GET /api/admin/traces`。
