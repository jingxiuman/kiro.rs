# 模型注册表最终修复：核验与补测报告

## 状态

**已完成，可提交。** 前一个 agent 的实现（288 行改动）未被推翻，仅补全了验证与规范。

## 核验结论（逐条对照验收标准）

### I1（§3.3 快照一致性）—— 已实现，核验通过

- `StreamContext::new_with_thinking` / `BufferedStreamContext::new` 的 `context_window: i32`
  已是**必填参数**（`stream.rs`），`BufferedStreamContext::set_context_window` 已删除，
  `handlers.rs` 两处调用点相应改为构造时传入。
- `stream.rs` 顶部 `use super::converter::get_context_window_size;` 已删除。
- `get_context_window_size()` / `map_model()`：生产代码零调用点，已用
  `grep -rn "get_context_window_size\|\\bmap_model(" src/` 核验——命中全部在
  `#[cfg(test)] mod tests` 内。`websearch_loop.rs` 使用的是
  `conversion.context_window`（`ConversionResult` 快照字段），不是这两个函数。
- 两函数均已加 `#[cfg(test)]` 门禁（原来只有注释说明「仅为兼容测试保留」，
  现在编译器强制），并补充了「本函数已无生产调用方，仅为既有测试保留」的文档注释。

### I2（§7.2 passthrough 告警节流）—— 已实现，本次补做红灯验证

- 实现已从「cap-and-clear」改为「cap-and-throttle」：`PassthroughWarnState` 记录
  `seen`（去重集合，容量硬顶 64）+ `last_flood_warn_at`（節流锚点）+
  `suppressed_since_flood_warn`（累计被抑制次数）+ `emitted`（测试可观测的实际
  warn 条数）。溢出后不再清空集合，改为按 60 秒节流发一条汇总 warn。
- **红灯验证（本次执行，产出如下）**：临时把 `note_passthrough_model` 溢出分支
  改回旧的 `state.seen.clear()`，运行新测试
  `passthrough_warn_throttles_flood_of_rotating_names`：

  ```
  thread '...passthrough_warn_throttles_flood_of_rotating_names' panicked at
  src/anthropic/model_registry.rs:1772:9:
  6500 次调用只应产生 <= 66 条 warn，实际 6500
  test result: FAILED. 0 passed; 1 failed
  ```

  证实新测试确实抓得住旧的「满 64 就清空」回归（65 个名字轮转 6500 次调用产生
  6500 条 warn，验证了「去重集合永远命中不了」的写放大）。随后原样撤回临时改
  动（`git diff --stat` 核对 `model_registry.rs` 改动行数与撤回前完全一致，
  仍是 +209），确认无残留。撤回后重跑该测试 + 全套 `passthrough_warn_*` 测试，
  全绿。

### M1（`ALLOW_PASSTHROUGH` 测试期线程本地覆盖）—— 已实现，核验通过

- `ALLOW_PASSTHROUGH_OVERRIDE`（`#[cfg(test)] thread_local!`）+
  `set_allow_passthrough()` 内的 `test_lock::held_by_current_thread()` 守卫断言，
  与既有 `install_registry` 同构；`TestLockGuard::drop` 同步清理该覆盖。
- 新增测试 `allow_passthrough_override_is_scoped_to_guard` 验证「装了对本线程生
  效、进程级全局不被写、出作用域即恢复默认 false」。
- 生产构建符号核验：`nm`/`strings` 对 debug 与 release 二进制都做了检查，
  `map_model|get_context_window_size|ALLOW_PASSTHROUGH_OVERRIDE|
  reset_passthrough_warn_state|MODEL_GLOBALS_TEST_LOCK` 零命中，断言字符串
  （`MODEL_GLOBALS_TEST_LOCK 必须`、`set_allow_passthrough() 必须`）计数为 0。

### M2（规范 §5.3 解析顺序）—— 本次补做

前序改动未触及规范文档。核对 `model_registry.rs::resolve()` 实际实现后发现
文档 §5.3 与代码有两处实质性偏差：

1. **步骤顺序错误**：文档原先把 `prefix` 匹配排在 `matchSubstrings` 之后（第 6
   步），但代码里 `prefix` 步骤实际排在「归一化匹配 upstreamId」**之前**（第 4
   步）——原因是归一化会剥掉日期后缀，若排在归一化之后，`gpt-5.6-sol-20250929`
   之类请求的日期会先被剥掉，`prefix` 命中时上游 id 就不再是原始请求名。
2. **遗漏一整个匹配步骤**：代码里存在「家族 + 版本宽松匹配」（第 7 步，复现旧
   `contains(家族) && contains(版本)` 语义，兼容 Bedrock/Vertex 前缀、
   `-latest`/`@日期` 后缀等），文档完全没有记录。

已按代码实际行为重写 §5.3 为 9 步顺序（alias → exposedId → {exposedId}-thinking
→ prefix → 归一化 upstreamId → matchSubstrings → 家族+版本宽松匹配 →
passthrough → reject），补充了宽松匹配的完整规则说明（家族/版本提取方式、
点号连字符两种版本形态、`sortOrder` 排序、已知与旧代码的可接受差异），并新增
「用户新增 prefix 行会遮蔽归一化 upstreamId 匹配」的注意事项（写入 §5.3 补充规
则 + §12 已知限制第 5 条）——因为 prefix 匹配现在排在归一化匹配之前，管理员若
新增一条 `exposedId` 与既有 `upstreamId` 前缀重叠的 prefix 行，会意外抢先命中。

## 测试结果

`cargo test --bin kiro-rs`，`CARGO_TARGET_DIR=/tmp/fix-core-target`：

| 次数 | 模式 | 结果 |
|---|---|---|
| 1 | 默认并发 | 636 passed; 0 failed; finished in 14.56s |
| 2 | 默认并发 | 636 passed; 0 failed; finished in 14.37s |
| 3 | 默认并发 | 636 passed; 0 failed; finished in 14.59s |
| 4 | `--test-threads=1` | 636 passed; 0 failed; finished in 32.44s |

基线较任务描述的「634+」略高（636），因为本次新增了两条测试
（`allow_passthrough_override_is_scoped_to_guard`、
`passthrough_warn_throttles_flood_of_rotating_names`，均来自前一个 agent 的改
动，非本次新增）。

## 生产构建符号核验

- `cargo build --bin kiro-rs`：0 错误（debug + release 均已构建）。
- debug 与 release 二进制均执行：
  `nm <bin> | grep -iE "map_model|get_context_window_size|ALLOW_PASSTHROUGH_OVERRIDE|reset_passthrough_warn_state|MODEL_GLOBALS_TEST_LOCK"`
  → 无命中（grep exit code 1）。
- `strings <bin> | grep -c "MODEL_GLOBALS_TEST_LOCK 必须\|set_allow_passthrough() 必须"`
  → debug 与 release 均为 0。

## 前人实现中发现的问题

- 无功能性问题。唯一的缺口是规范文档未同步（M2），已补齐。
- 一个可接受的技术债（非本次修复范围）：新增 `prefix` 行与既有行的前缀冲突在
  加载校验阶段未做检测，已记录为已知限制（§12 第 5 条），留给后续迭代。

## 顾虑

- 未新增针对「用户新增 prefix 行遮蔽归一化匹配」这一行为的自动化测试（任务只
  要求补规范文档，未要求补代码/测试；且这是一个配置错误场景，不是当前分支要
  修的行为缺陷，加载校验的冲突检测本身是新功能，超出本次「核验 + 补验证 + 补
  规范」的范围，故未做，仅记录为已知限制）。
- release 构建耗时较长（约 43s 编译 + 后续 nm/strings 检查），未纳入常规 CI
  验证流程，仅作为本次符号核验的一次性动作。
