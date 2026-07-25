# Task 6 修复报告：model_registry.rs resolve() 行为回归

## 背景

改造前 `converter.rs::map_model()` 用 `contains()` 在整个字符串上做家族/版本
匹配，天然容忍模型名前后噪音（Bedrock/Vertex 风格前缀、`-latest` 等后缀）。
改造后 `ModelRegistry::resolve()` 改成「规范化 + 精确匹配」，导致这些真实
存在的 id 形态从 `Some` 变成 `None`（下游 400）；另外 `gpt-5.6-sol-20250929`
这类应原样透传的 id，日期后缀被规范化剥掉了。

## 修改内容（仅 `src/anthropic/model_registry.rs`）

### 修改 1：prefix 匹配前移

`resolve()` 的匹配顺序由：

```
alias → exposed_id 精确 → exposed_id-thinking → 规范化匹配 upstream_id
→ match_substrings → prefix → passthrough → Unknown
```

调整为：

```
alias → exposed_id 精确 → exposed_id-thinking → prefix（新位置）
→ 规范化匹配 upstream_id → match_substrings → 家族+版本宽松匹配（新增）
→ passthrough → Unknown
```

原因：规范化会剥掉日期后缀（`-YYYYMMDD`），但 `gpt-5.6-sol-20250929` 这类
gpt-5* 请求要求原样透传、日期不能丢。若规范化先跑，prefix 步骤命中时上游 id
已经不是原始请求名了。claude 系没有 prefix 行，不受影响。

### 修改 2：新增「家族 + 版本」宽松匹配步骤（第 7 步）

在 `match_substrings` 之后、passthrough 之前插入。匹配规则完全从
`upstream_id` 自动派生，不新增数据字段：

- 仅对 `match_kind == Exact` 且 `upstream_id` 以 `"claude-"` 开头、且按 `-`
  切分的最后一段形如 `<数字>.<数字>` 的行启用（如 `claude-opus-4.8` 的
  `"4.8"`）。**刻意排除**无点号的版本段（`claude-sonnet-5` / `claude-fable-5`
  的 `"5"`），否则 `contains("sonnet") && contains("5")` 会让
  `claude-3-5-sonnet` 被误判为 sonnet 5 代（旧行为是 `None`）——这些行的
  宽松别名已由既有 `match_substrings` 单独覆盖。
- 家族关键字 = `upstream_id` 按 `-` 切分的第 2 段。
- 版本形态两种：点号版（`4.8`）与连字符版（`4-8`），命中其一即可。
- 命中条件：请求名同时包含家族关键字 + 其中一种版本形态。
- 多行命中时按 `sort_order` 升序取第一个（用 `min_by_key` 显式实现，不依赖
  vec 遍历顺序，因为覆盖层加载后的行顺序不保证等于 sort_order 顺序）。

**额外发现并修复的联动 bug**：新增的第 7 步宽松匹配最初直接返回
`Mapped`，绕过了第 3 步为 `-thinking` 请求记录的「变体关闭→待定拒绝」语义
——导致 `expose_thinking_variant = false` 的行遇到
`claude-opus-4-8-thinking` 请求时被第 7 步的宽松匹配误判为可用（因为宽松
匹配不检查 `-thinking` 后缀本身是否合法）。修复：第 7 步命中后，若请求名
以 `-thinking` 结尾，同样过一遍 `enabled` / `expose_thinking_variant` 门禁，
语义与第 3/5 步一致。这是在跑 `thinking_variant_disabled_rejects_thinking_request`
与 `thinking_disabled_still_rejects_when_no_fallback` 两条既有测试时发现的
真实回归，已修复并保留在最终实现中。

## 已知可接受差异（写在代码注释里）

旧代码 opus 分支判断顺序是 `4-8 → 4-7 → 4-5 → 4-6`（`4-5` 排在 `4-6` 之前，
像笔误但是既有事实），新实现按 `sort_order` 即 `4.8 → 4.7 → 4.6 → 4.5`。
只有当输入同时包含两个不同版本号（如 `claude-opus-4-5-4-6` 这种病态输入）
时结果才不同，属可接受差异。

## 新增测试

在 `src/anthropic/model_registry.rs::tests` 中新增 4 条测试（均通过）：

- `family_version_loose_match_tolerates_prefixes_and_suffixes`
- `loose_match_still_rejects_legacy_three_five_sonnet`
- `gpt5_passthrough_preserves_date_suffix`
- `exact_match_wins_over_loose_match`

## 差分验证（临时模块，已删除）

在 `model_registry.rs` 中临时加入 `#[cfg(test)] mod diff_regression`，
将 `git show 744b2a9~1:src/anthropic/converter.rs`（744b2a9 是
「三个硬编码表改为查注册表」重构提交，`~1` 为改造前的父提交）中的
`map_model()` 与 `get_context_window_size()` 原样拷贝为参考实现，逐条比对
以下语料：

- 3 前缀（``/`anthropic.`/`us.anthropic.`）
  × 5 家族（opus/sonnet/haiku/fable/gpt）
  × 9 版本段（`4-5/4-6/4-7/4-8/5/4.5/4.6/4.7/4.8`）
  × 4 后缀（``/`-thinking`/`-latest`/`-20250929`/`-v1:0`，共 5 种，spec
  描述为 4 但实际枚举了 5 种全部纳入）
  = 675 条组合（gpt 家族不区分版本段，退化为固定基底 `gpt-5.6-sol` 复测
  前后缀维度）
- 23 个既有 exposed id
- 既有测试文件中出现过的全部输入（34 条）

去重后语料规模：**579 条**。

比对结果：**差异数量 0**。运行输出：

```
语料规模: 579 条
差异数量: 0
test anthropic::model_registry::diff_regression::diff_against_pre_change_converter ... ok
```

未观察到需要人工判断可接受性的剩余差异（唯一预期的可接受差异——opus
`4-5`/`4-6` 顺序冲突——需要病态输入如 `claude-opus-4-5-4-6` 才会触发，
未包含在本次系统化生成的语料中；已在代码注释中说明该差异的存在与原因）。

验证完毕后临时模块 `diff_regression` 已整体删除（`git status` 确认干净，
仅剩对 `model_registry.rs` 的正式修改）。

## 测试结果

`cargo test --bin kiro-rs`：**560 passed, 0 failed**（含新增 4 条测试，
无任何既有测试被修改或失败）。`cargo test --bin kiro-rs model_registry`
单独运行：**38 passed, 0 failed**。
