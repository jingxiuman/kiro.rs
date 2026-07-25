# passthrough 命中告警去重（有界集合）—— 完成报告

## 改动文件

- `src/anthropic/model_registry.rs`
  - 新增 `PASSTHROUGH_WARN_CACHE_CAP`（=64）常量、`PASSTHROUGH_WARN_CACHE`（`LazyLock<RwLock<Vec<String>>>`）全局去重集合。
  - 新增 `pub fn note_passthrough_model(model: &str)`：命中则跳过；未命中且容量已满则整体 `clear()` 后再 push（粗粒度节流，避免无界增长）；未命中且未满则记入并打一条 `tracing::warn!`。
  - 新增 `#[cfg(test)] pub fn passthrough_warn_cache_len()` 供测试读取集合大小。
  - 复用文件内已有的 `LazyLock` / `parking_lot::RwLock` import（定义在文件尾部，Rust 中 `use` 声明模块级生效，不受文本位置先后影响，无需重复引入）。
  - 新增两个测试 `passthrough_warn_dedup_is_bounded`、`passthrough_warn_dedups_same_model`，均取现有 `REGISTRY_TEST_LOCK` 而非新建锁（见下方「锁的选择」）。

- `src/anthropic/converter.rs`
  - `convert_request_with_mode` 中原先 `Resolution::Mapped { .. } | Resolution::Passthrough { .. } => (upstream_id, context_window)` 的合并分支拆分为两个独立分支：`Mapped` 分支行为完全不变；`Passthrough` 分支在取出 `(upstream_id, context_window)` 前多调用一次 `super::model_registry::note_passthrough_model(&req.model);`。
  - 未改变任何返回值构造逻辑，只新增一次副作用调用。

## 锁的选择及理由

沿用现有 `REGISTRY_TEST_LOCK`，未新增专用锁。

理由：虽然 `PASSTHROUGH_WARN_CACHE` 与 `REGISTRY`/`ALLOW_PASSTHROUGH` 在物理上是三个独立的 `static`，但本任务新增的两个测试都会真实调用 `note_passthrough_model`（进而写全局 `PASSTHROUGH_WARN_CACHE`），而这两个测试的断言依赖「进入测试前集合处于确定/可控状态」的相对关系（`before` / `after_first` / `after_second` 的增量关系），并非要求绝对值。经过手工推演：
- 若 `passthrough_warn_dedup_is_bounded` 先跑，200 次不重复 model 名从空集合开始，按 `((200-1) % 64) + 1 = 8` 计算，退出时集合固定为 8 个元素（确定性，不依赖并发穿插）；
- 之后 `passthrough_warn_dedups_same_model` 无论以哪个起点开始，其断言只看「加一个新名字前后差 1」「重复加不变」，与绝对长度无关，恒成立。
唯一会真正冲突的场景是「两个测试的读写操作被其他线程交错执行」，而不是「共享同一把锁导致的过度串行化」。由于本文件里目前只有这两个新测试和 `install_and_read_global_registry` 会碰触进程级可变状态，复用 `REGISTRY_TEST_LOCK` 成本可忽略（三个测试合计远小于 1 秒），额外引入一把锁除了多一层心智负担外没有可衡量收益，按 R3「加之前先减」判断不新增。

若未来 `PASSTHROUGH_WARN_CACHE` 相关测试显著增多、且与 `install_registry` 测试出现明显的串行化瓶颈，再拆分专用锁（同样需要 `unwrap_or_else(|e| e.into_inner())` 防中毒）不迟。

## 测试命令与实际输出

环境：
```
export CARGO_TARGET_DIR=/tmp/target-passthrough-dedup
```
（独立 target 目录，avoid 与其他 agent/构建共享导致的 cargo 指纹失真）

```
$ cargo test --bin kiro-rs
...
test result: ok. 575 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 14.24s
```

573（基线）+ 2（新增）= 575，与预期一致，0 失败。

单独跑新增两个测试确认其真实生效（非碰巧被基线其他测试掩盖）：
```
$ cargo test --bin kiro-rs passthrough_warn
running 2 tests
test anthropic::model_registry::tests::passthrough_warn_dedup_is_bounded ... ok
test anthropic::model_registry::tests::passthrough_warn_dedups_same_model ... ok
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 573 filtered out; finished in 0.00s
```

`cargo build --bin kiro-rs` 单独跑通过（1m30s，含 vendored OpenSSL 首次编译）。

## 自查发现的问题

- 未发现需要修正的问题。`git diff --stat` 确认只改了两处被授权的文件（`src/anthropic/model_registry.rs`、`src/anthropic/converter.rs`），未触碰其他文件。
- 确认了拆分 `Mapped`/`Passthrough` 分支后两者返回值构造语句完全一致（仅 `Passthrough` 分支多一行 `note_passthrough_model` 调用），不改变既有行为，回归测试 575 passed / 0 failed 印证了这一点。
- `note_passthrough_model` 中"命中则跳过；满则清空重来"的策略是有意的粗粒度节流（而非 LRU 或时间窗口），在需求描述中已被指定为可接受方案，未做额外设计。
