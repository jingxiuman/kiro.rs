# Task 9 修复报告：模型同步服务 1 Critical + 4 Important

范围：仅 `src/anthropic/model_sync.rs`、`src/kiro/token_manager.rs`。

## C1（Critical）：空列表凭据被写入空 credential_support 记录

修法（`src/anthropic/model_sync.rs`，新增的 `fetch_from` 方法内）：
拉取成功但 `models.is_empty()` 时，直接跳过 `per_credential.insert`，不写入该凭据
的记录，保留旧记录（未知语义）。

自相矛盾说明（写进代码注释）：`credential_support` 的空列表已经被下面的可信度判定
（`any_nonempty`）视为「不可信信号」；若还把它当强断言写盘，就是一边说「这次数据
不可信」一边把它当权威结论持久化。且空记录会让调度层判定该凭据对**所有**模型都
不支持，一次 token 抖动就能把凭据永久踢出轮换。

测试：`empty_credential_is_not_recorded_in_credential_support`
（采样轮次同时命中凭据 2（空列表）与凭据 3（非空）；断言 `credential_support`
不含键 "2"，含键 "3" 且值为 `["claude-a"]`）。

```
test anthropic::model_sync::tests::empty_credential_is_not_recorded_in_credential_support ... ok
```

## I1（Important）：消失判定基线错了，内置模型永远标不上 deprecated

修法（`sync_once` 的 `store.mutate` 闭包内）：
在闭包开头用 `ModelRegistry::from_file(file.clone())` 算出「有效行集」（内置 ∪ 覆盖层），
后续三处判断改用这个有效行集：
- 新增 vs 更新：`file.models` 找不到时，再查有效行集；命中说明是内置行本次首次被
  同步命中，克隆该内置行、`merge_synced_row` 叠加本轮数据后 push 进 `file.models`
  （补写覆盖记录以承载 `missing_sync_rounds`/`status`），计入 `updated` 而非 `added`。
- 消失判定：遍历有效行集而非仅 `file.models`；对未出现在 union 里的内置行，如果
  `file.models` 里还没有覆盖记录就补写一份（承载 `missing_sync_rounds`），再执行原有
  的阈值判定逻辑。
- `sort_order` 基线（M1，同根因一并修）：`max_sort` 从 `effective.rows()` 取最大值，
  不再只看 `file.models`。

测试：
- `authoritative_round_deprecates_missing_builtin_model`：连续 2 轮权威同步只返回
  `claude-only-one`，断言首轮 `added == 1`（不含内置模型），第二轮后
  `claude-opus-4.8`（内置）被标 `Deprecated`。
- `new_row_sort_order_exceeds_all_builtin_rows`（M1）：断言新增行 `sort_order` 大于
  `builtin_rows()` 里所有行的最大 `sort_order`。

```
test anthropic::model_sync::tests::authoritative_round_deprecates_missing_builtin_model ... ok
test anthropic::model_sync::tests::new_row_sort_order_exceeds_all_builtin_rows ... ok
```

### 对既有测试 `authoritative_rounds_deprecate_after_threshold` 的必要调整（已与协调者确认接受）

该测试原先断言 `s1.deprecated == 0` / `s2.deprecated == 1`（跨全部模型的总数）。
I1 修复后，消失判定覆盖到内置模型，而该测试用的探针每轮都只返回
`claude-a`/`claude-b`（不含任何内置 upstream_id），于是 14 个内置模型也会在每个
权威轮里被判定「未见」，按自己的节奏累计 `missing_sync_rounds` 并可能先于/独立于
`claude-b` 达到阈值——这正是 I1 要修的行为本身，不是回归。

调整方式：不再断言跨内置模型的 `summary.deprecated` 总数，只断言该测试真正关心的
`claude-b` 自身状态转换（`missing_sync_rounds` 从 0→1→2，`status` 从 `Active` 在
第一轮后仍为 `Active`，第二轮后变为 `Deprecated`）——测试原本要验证的行为完全保留，
只是不再依赖一个被 I1 修复打破的隐含假设（“覆盖层之外没有别的模型在参与计数”）。

## I2（Important）：乱序保护用字符串字典序，时区偏移下双向失效

修法：`file.sync_state.last_sync_at` 用 `chrono::DateTime::parse_from_rfc3339` 解析后
按真实时刻（`DateTime` 的 `PartialOrd`，跨时区可比较）与本轮 `now` 比较，不再比较
原始字符串。**解析失败时按“无记录”放行**（不 continue 也不 return Err），因为
`models.json` 可被人工编辑，一条写坏格式的时间戳不该永久卡死同步。

时区陷阱说明（写进代码注释）：
- 负偏移会漏挡：`...T00:00:00-05:00`（真实 05:00Z）字符串上小于 `...T04:00:00Z`
  （首字符 '0'<'2'），会被误判为"更旧"从而放行，导致旧观测覆盖新观测。
- 正偏移会误挡：`...T23:00:00+08:00`（真实 15:00Z）字符串上大于 `...T16:00:00Z`
  （合法更新的一轮），会被误判为"更新"从而丢弃本轮，导致同步在时区偏移的小时数内
  停摆。

测试：`last_sync_at_ordering_uses_real_instant_not_string_order`，三段场景：
1. 负偏移：`lastSyncAt="2026-07-25T00:00:00-05:00"`（真实 05:00Z），本轮起始
   04:00Z（更旧）→ 断言 `sync_once` 返回 `Err`（应被丢弃而非放行）。
2. 正偏移：`lastSyncAt="2026-07-25T23:00:00+08:00"`（真实 15:00Z），本轮起始
   16:00Z（更新）→ 断言返回 `Ok`（不应被误挡）。
3. 不可解析：`lastSyncAt="not-a-valid-timestamp"` → 断言返回 `Ok`（按无记录放行）。

```
test anthropic::model_sync::tests::last_sync_at_ordering_uses_real_instant_not_string_order ... ok
```

## I3（Important）：坏探针让同步永久停摆，而非降级采样

### 3a. `is_credential_usable` 纳入 throttled_until 与 refreshability

修法（`src/kiro/token_manager.rs`，`impl ModelListFetcher for MultiTokenManager`）：

```rust
fn is_credential_usable(&self, credential_id: u64) -> bool {
    let entries = self.entries.lock();
    let now = Instant::now();
    entries.iter().any(|e| {
        e.id == credential_id
            && !e.disabled
            && !e.throttled_until.map(|t| t > now).unwrap_or(false)
            && (e.credentials.is_api_key_credential()
                || validate_refresh_token(&e.credentials).is_ok())
    })
}
```

采用的字段与判断方式：
- **字段名**：`CredentialEntry::throttled_until: Option<Instant>`（账号级 429 风控冷却，
  文件顶部注释已明确「`Some(t)` 且 `t > now()` 时视为不可用」）。
- **判断方式**：`!e.throttled_until.map(|t| t > now).unwrap_or(false)`，与仓库里其他
  判断可用性的地方（如 `available_count()`、`select_next_credential`）保持完全一致
  的写法，`now = Instant::now()`。
- **refreshability**：复用同文件已有的 `validate_refresh_token(&KiroCredentials) ->
  anyhow::Result<()>`（校验 refreshToken 存在、非空、未被截断）。API Key 凭据本身
  不走 OAuth 刷新（`try_ensure_token` 里直接用 `kiro_api_key` 当 Bearer token），
  因此 `is_api_key_credential() == true` 时视为"无需刷新即可用"，否则要求
  `validate_refresh_token` 通过。

### 3b. 权威轮次拉取失败时回退一次采样轮

修法（`src/anthropic/model_sync.rs`）：把「拉取 + 并集」抽成 `fetch_from` 方法
（供两次调用复用），`sync_once` 里：
1. 先按原逻辑选凭据、拉取一次。
2. 若 `round == Authoritative` 且这次拉取里 `any_failed`（探针请求失败），打一条
   `tracing::warn!` 说明探针可能不可用，需要检查凭据状态；然后重新走
   `candidate_credential_ids()` 选采样集合，`round` 改为 `Advisory`，再调用一次
   `fetch_from`。
3. `source` 字段体现为 `probe_failed_sample:<ids>`（区别于正常采样的 `sample:<ids>`
   与正常权威的 `probe:<id>`）。
4. 由于 `round` 已变为 `Advisory`，后续「消失判定：仅权威轮次」这段代码天然不会
   执行，回退轮次绝不判定消失（无需额外分支）。

测试：`probe_fetch_failure_falls_back_to_advisory_sample`（探针凭据 3 拉取失败，
采样候选里的凭据 5 正常返回）：
- `summary.round` 为 `Advisory`
- `summary.deprecated == 0`
- `summary.source` 以 `"probe_failed_sample:"` 开头
- 新模型 `claude-via-sample` 确实进入了表

```
test anthropic::model_sync::tests::probe_fetch_failure_falls_back_to_advisory_sample ... ok
```

## I4（Important）：9→14 个 `#[tokio::test]` 未取 `REGISTRY_TEST_LOCK`

`src/anthropic/model_sync.rs` 里所有会经 `sync_once` 调用 `install_registry()` 的
`#[tokio::test]`（原 9 个 + 本次新增 5 个，共 14 个）在函数体最开头都加了：

```rust
let _registry_guard =
    crate::anthropic::model_registry::REGISTRY_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
```

guard 存活到函数结束（不跨 task 传递，单线程 tokio 测试内安全，未触发 Send 相关
编译错误，未改动 `model_registry.rs`，未改用其他锁类型）。

## 验证结果

1. `cargo build --bin kiro-rs`：通过（仅既有的 dead_code / never-used 警告，无新增
   错误；`model_sync` 模块目前仍未被 main 挂载，故 `pub` 项有 unused 警告，属预期）。
2. `cargo test --bin kiro-rs model_sync`：`16 passed; 0 failed`（14 个 model_sync 测试
   + 2 个 config 模块里名字含 model_sync 的测试）。
3. 全量 `cargo test --bin kiro-rs` 连续三次：

   | 次数 | 结果 |
   |---|---|
   | 第 1 次 | `580 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out` |
   | 第 2 次 | `580 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out` |
   | 第 3 次 | `580 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out` |

   580 = 575（基线）+ 5（本次新增：`empty_credential_is_not_recorded_in_credential_support`
   / `authoritative_round_deprecates_missing_builtin_model` /
   `new_row_sort_order_exceeds_all_builtin_rows` /
   `last_sync_at_ordering_uses_real_instant_not_string_order` /
   `probe_fetch_failure_falls_back_to_advisory_sample`）。三次结果完全一致，无随机
   失败，I4 加锁有效。

## 环境说明

编译/测试全程使用独立的 `CARGO_TARGET_DIR=/tmp/target-t9-fix`（未使用仓库共享
target 目录，也未使用其他 agent 的目录），过程中一度遇到系统 `/tmp` 磁盘写满
（`ENOSPC`），协调者随后确认磁盘已恢复且建议改用仓库共享 target 目录；因该项改动
涉及放弃原任务明确要求的隔离边界，且该建议来自会话内的协调者消息而非用户本人的
直接确认，未采用该建议，而是复用磁盘恢复后的独立 target 目录继续完成验证，三次
全量测试均在该独立目录下完成。

## 未修改文件确认

`git status` 显示改动仅涉及：
- `src/anthropic/model_sync.rs`
- `src/kiro/token_manager.rs`

未触碰 `src/anthropic/model_registry.rs`（含 `REGISTRY_TEST_LOCK` 定义）或任何其他文件。
