# 凭据↔代理批量重绑 设计

日期：2026-08-21
状态：已与用户对齐（对话确认：交付面 = API + 管理面板 UI；语义 = 全有或全无）

## 背景与目标

代理池已有批量添加（`POST /proxy-pool/batch`）、全量健康检查、round-robin 批量分配。
缺的是**显式指定映射**的批量重绑：管理员想一次提交「这几张凭据用这个代理」的完整
映射表，而不是逐张凭据调 `POST /credentials/{id}/proxy`，也不是接受 round-robin
的自动均摊结果。

规模按现状设计：4 代理 / 8 凭据，同一台自建代理机。界面与校验按**几十条以内**
量级做，不做分页/搜索/流式导入。以后接大池子再改——现在做是无收益的复杂度（R3）。

## API

### `POST /api/admin/credentials/proxy/batch`

请求体：

```json
{
  "assignments": [
    { "credentialId": 1, "proxyId": 3 },
    { "credentialId": 2, "proxyId": null }
  ]
}
```

- `proxyId: null` = 解绑该凭据的专属代理，回落全局代理（与现有单条接口的清除语义一致）。
- 未出现在列表里的凭据**不动**——这是「批量修改」不是「全量替换」。

响应（成功）：

```json
{ "success": true, "message": "已更新 2 张凭据的代理绑定" }
```

响应（校验失败，HTTP 400）：

```json
{
  "error": "批量重绑校验失败，未应用任何变更",
  "failures": [
    { "credentialId": 99, "reason": "凭据不存在" },
    { "credentialId": 2, "reason": "代理 #7 不存在或已禁用" }
  ]
}
```

### 语义：全有或全无

1. **先整体校验**：每条的 `credentialId` 必须存在；`proxyId` 非 null 时必须存在于
   代理池且 `enabled=true`（`autoDisabled` 的代理视为不可选——绑一个已被健康检查
   踢掉的代理没有意义）；同一 `credentialId` 在列表里出现多次视为校验失败（歧义输入
   不猜测意图）。
2. 任一条非法 → 整批拒绝，返回**全部**失败条目（不是第一条），不落任何变更。
3. 全部合法 → 单次锁内逐条应用，**落盘一次**。

选择全有或全无而非部分成功：8 条量级下部分成功的恢复语义（哪些成了哪些没成、
要不要重试剩余）比重新提交一次贵得多。

### 实现落点

- `service.rs`：新增 `assign_proxies_batch(...)`，复用现有
  `assign_proxy_to_credential`（`service.rs:2816`）的单条校验与写入逻辑——抽出
  不落盘的内层函数供两者共用，避免复制校验代码。落盘沿用现有凭据保存路径。
- `handlers.rs` + `router.rs`：新 handler + 路由，形态照抄
  `assign_proxies_round_robin`（`handlers.rs:496`）。
- 类型：`AssignProxyBatchRequest { assignments: Vec<AssignmentEntry> }`，
  `AssignmentEntry { credential_id: u64, proxy_id: Option<u64> }`。

## 管理面板 UI

凭据页工具栏加「批量绑代理」按钮 → 弹窗：

- 列出**全部**凭据（8 条量级，不分页），每行：凭据 email/ID + 代理下拉框。
- 下拉选项来自 `GET /proxy-pool`：仅列 `enabled && !autoDisabled` 的代理，
  显示 `#id url（健康状态, 延迟ms）`；另有「跟随全局代理」项（= null）。
- 预填当前绑定；**未改动的行不进请求体**（对应 API 的「未出现则不动」语义）。
- 提交后：成功 toast + 刷新凭据列表；400 时在对应行内联展示 reason，不关弹窗。
- 无改动时提交按钮置灰。

## 错误处理

- 后端校验失败：见上，400 + 全部失败条目。
- 落盘失败（IO）：500，此时内存态可能已更新——沿用现有单条接口在该场景下的行为，
  不为本功能单独引入回滚机制（现状单条接口同样如此，保持一致）。
- 前端网络失败：弹窗内提示重试，不清空用户已选内容。

## 测试

- service 层单测：
  - 全合法 → 全部生效且只落盘一次
  - 一条凭据不存在 → 整批拒绝、已存在绑定不变、failures 含该条
  - 一条 proxy 被禁用 → 同上
  - 重复 credentialId → 校验失败
  - `proxyId: null` → 清除绑定
- handler 层：400 响应体结构断言（failures 数组完整性——两条非法要报两条）。
- UI 不做自动化测试（面板现状无测试框架），部署后手工验收：改 2 条提交 →
  面板刷新后绑定正确 → `data/credentials.json` 落盘值正确。

## 明确不做

- 部分成功语义、分页/搜索、按代理反向批量（「把这个代理下的所有凭据换到那个」）、
  绑定时即时连通性探测（健康检查已在后台周期跑）。
