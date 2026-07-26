# 模型注册表最终修复报告（评审 §6.3 终版落地）

## 结论

后端 5 条验收标准（C1、C2、I1、M1、M2）在接手时**均已实现**，实现质量高、注释详实。
本轮工作内容：核实、补测试（原实现完全没有覆盖 C1/C2/I1/M1/M2 的直接测试）、
修复一处会导致 `cargo test` 无法编译的遗留问题、补前端（护栏横幅 + M3）、验证、提交。

## 接手时发现的问题（前一个 agent 的实现缺陷）

**测试代码编译失败**（不是这次任务清单里的条目，是核实过程中发现的隐藏 bug）：
`src/admin/service.rs:4363` 的 `sync_refresh_changes_credential_filtering_end_to_end`
测试仍调用旧签名 `service.sync_models()`（0 参数），但 `sync_models` 的签名已经在
本次改动中变成 `sync_models(&self, force_disappearance_check: bool)`。
`cargo build --bin kiro-rs`（不含测试）能过是因为这条测试代码只在 `cfg(test)` 下编译，
`cargo build` 根本不会碰到它——**这也是「build 通过」不能替代「test 通过」的活例子**。
修复：调用点改为 `service.sync_models(false)`（不强制放行，保持该测试原有语义不变）。

## 逐条核实

- **C1**：`SyncSummary` / `ModelRegistryResponse` 均带 `disappearance_check_skipped: bool` +
  `missing_ratio: f64`（camelCase 序列化后为 `disappearanceCheckSkipped` /
  `missingRatio`）。强制放行入口是 **`POST /models/sync?force=true`**（查询参数，
  风格对齐既有 `DELETE /groups/{name}?force=true`）。护栏结论编码进
  `syncState.source`（`encode_source`/`decode_source`），因为定时同步不经 admin 层、
  重启也会丢内存态，只有落盘字段能让 UI 持续看到「消失判定已停机」。已实现，已补测试。
- **C2**：`in_baseline` 判据（非 synced，或已有 `model_meta` 记录）已实现，配合
  `may_confirm_new_rows` 阻止「护栏拦下的轮次新增的行」污染下一轮分母。已实现，已补测试
  （三轮序列复现评审实测的 r0/r1/r2 症状，确认 r1/r2 不再误杀）。
- **I1**：`exposed_names_of` + `taken_names` 集合在写入前做冲突检测，冲突则跳过该行、
  打 `tracing::error!`，不返回 `Err`。已实现，已补测试（撞名行被跳过 + 下一轮同步仍正常）。
- **M1**：`SetModelSyncSettingsRequest` 与 `CreateModelRequest` 均已加
  `#[serde(flatten)] extra: BTreeMap<String, Value>`，非空时在 service 层返回
  `InvalidModelField`。已实现，已补测试（未知字段 400，且不留部分写入）。
- **M2**：settings 时间校验错误已改用 `InvalidModelField`，文案为「模型同步时间无效」，
  不再提及「凭据无效」或「自动更新」。已实现，已补测试。

## 新增测试（红→绿，均已验证为绿）

`src/anthropic/model_sync.rs`：
1. `force_override_resumes_disappearance_check_after_guard_pause`（C1）：
   上游稳定只保留 5/13，连跑 20 个权威轮，断言 `missing_sync_rounds` 全程为 0、
   `disappearance_check_skipped=true`；随后 `force=true` 跑两轮，第一轮只累计到 1，
   第二轮达阈值正常标记 Deprecated。
   **注意一个与原始任务措辞的偏差**：任务描述里写「强制放行一轮 → 断言…模型被标
   Deprecated」，但 `MISSING_ROUNDS_THRESHOLD=2` 且 force 只绕过比例护栏、不绕过
   这个阈值（这是代码里显式的设计决定，注释写明了原因：force 不应该让「一次抖动」
   跳过「连续两轮才作数」的既有保护）。按字面「一轮」实现会导致断言失败，或者
   必须削弱阈值语义才能通过——两者都不对。因此测试改为「强制放行两轮」，第一轮
   断言「计数恢复正常累计」，第二轮断言「达阈值后正常退役」，如实反映代码行为。
2. `guard_denominator_not_diluted_by_rows_added_under_paused_guard`（C2）：
   探针只返回 20 个全新陌生 id（一个内置行都不返回），三轮序列断言 r0/r1/r2
   护栏持续触发、内置行不被误杀（对照评审实测的旧 bug：r1 分母被撑大到 33、
   r2 整表 13 行误杀）。
3. `conflicting_exposed_id_is_skipped_not_fatal`（I1）：两个上游 id 派生出同一个
   exposedId，断言只有一行被写入、整轮 `trusted=true`，且下一轮同步仍能正常进行
   （自愈路径，不会永久停摆）。

`src/admin/service.rs`（`model_registry_tests` 模块）：
4. `create_model_rejects_unknown_fields`（M1）：`POST /models` 未知字段 → 400，
   不留部分写入。
5. `set_model_sync_settings_rejects_unknown_fields`（M1）：`PATCH /models/settings`
   未知字段 → 400，错误信息点名字段。
6. `set_model_sync_settings_invalid_time_uses_model_field_error_not_credential_wording`
   （M2）：非法时间格式走 `InvalidModelField`，文案不含「凭据无效」「自动更新」。

以上 6 条 + 原有 634 条，`cargo test --bin kiro-rs` 现为 **640 passed**，连续 3 次运行
结果一致（见下方「验证」）。**既有测试一条未改**，只在 `service.rs:4363` 补了一个必需
参数（旧测试逻辑与断言完全不变）。

## 前端改动

- `admin-ui/src/types/api.ts`：`ModelRegistryResponse` / `SyncSummary` 补
  `disappearanceCheckSkipped: boolean` + `missingRatio: number`。
- `admin-ui/src/api/models.ts`：`syncModels(force = false)`，`force=true` 时带
  `?force=true` 查询参数。
- `admin-ui/src/hooks/use-model-registry.ts`：`useSyncModels()` 的 `mutationFn`
  接受显式 `force: boolean`（未用 JS 默认参数——TanStack Query 对单参数带默认值的
  `mutationFn` 会把 `TVariables` 推断成 `void`，导致 `mutate(true)` 类型报错；
  改成显式非默认参数即可，调用方（横幅按钮）自己传 `true`/`false`）。
- `admin-ui/src/components/model-mapping-dialog.tsx`：
  - `RegistryBanners` 新增护栏横幅：`disappearanceCheckSkipped` 为真时显示
    缺失比例、两种可能原因（探针配错 / 上游真下线），提供「确认探针无误，本轮
    放行消失判定」按钮，点击调用 `syncModels(true)`。
  - **M3**：`TextField` / `NumberField` 的 `onApply` 签名加 `onRejected?: () => void`，
    `ModelRowCard.apply` 的 `onError` 里调用它，两个字段组件的 `commit()` 分别传入
    `() => setDraft(value)` / `() => setDraft(String(value))`。此前只有本地校验分支
    （空值 / 非正整数）会回滚草稿，服务端拒绝后草稿停留在非法值上，界面与实际保存值
    不一致；现在服务端拒绝同样触发回滚。
  - `NumberField` 数字校验加 i32 上界 `next <= 2147483647`（后端字段是 i32），
    超出提前本地拦截并回滚，避免多一次无意义往返。

## 验证

- `cargo test --bin kiro-rs`：连续 3 次运行，均为 `640 passed; 0 failed`（基线 634 + 新增 6）。
- `cd admin-ui && npm run build`：`tsc -b && vite build` 成功，产物正常生成。
- `admin-ui/package-lock.json`（本仓库锁文件是 `bun.lock`，多余文件）已删除。

## 强制放行入口的形式

**`POST /models/sync?force=true`** 查询参数（`SyncModelsQuery { force: bool }`），
风格对齐既有 `DELETE /groups/{name}?force=true`。默认 `false`；只绕过比例护栏，
不绕过 `MISSING_ROUNDS_THRESHOLD=2`。

## 顾虑 / 遗留

1. **C1 测试与任务原始措辞的偏差**：见上文「新增测试」第 1 条的说明。这是我基于
   代码里显式写明的设计决定（force 不绕过 2 轮阈值）做出的判断，不是我自己引入的
   新语义——如果这个判断有误，应该是先质疑「force 该不该也绕过 2 轮阈值」这个设计
   决定本身，而不是让测试屈从于字面描述。
2. **未做前端组件级/E2E 测试**：`admin-ui` 目录下没有找到既有的前端测试基础设施
   （未见 vitest/jest 配置或 `*.test.tsx` 文件），本次前端改动仅以 `npm run build`
   (`tsc` 类型检查 + vite 构建) 验证，未做运行时行为的自动化测试。如需要，这是
   一块需要先补测试基础设施才能做的后续工作。
3. **未跑 `npm run dev` 实际点击验证横幅 UI**：受限于沙箱内没有跑一个真实后端
   触发护栏场景的环境，横幅的视觉/交互只做了代码走读级别的核对，没有截图验证。
