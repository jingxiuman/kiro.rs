import axios from 'axios'
import { storage } from '@/lib/storage'
import type {
  CreateModelRequest,
  ModelRegistryResponse,
  PatchModelRequest,
  SetModelSyncSettingsRequest,
  SyncSummary,
} from '@/types/api'

// 与 api/credentials.ts、api/groups.ts 一致：每个模块自建 axios 实例 + 同样的拦截器
const api = axios.create({
  baseURL: '/api/admin',
  timeout: 30000, // 同步会实打实打上游，比其他管理接口慢，放宽到 30s
  headers: { 'Content-Type': 'application/json' },
})

api.interceptors.request.use((config) => {
  const apiKey = storage.getApiKey()
  if (apiKey) config.headers['x-api-key'] = apiKey
  return config
})

/** 读取全表：模型行 + 别名 + 同步状态 + 同步设置 + 降级信息 */
export async function getModelRegistry(): Promise<ModelRegistryResponse> {
  const { data } = await api.get<ModelRegistryResponse>('/models')
  return data
}

/** 手动触发一轮同步，返回 diff 摘要（不返回全表，需另行刷新） */
export async function syncModels(): Promise<SyncSummary> {
  const { data } = await api.post<SyncSummary>('/models/sync')
  return data
}

/** 新增一行手动模型。后端返回改动后的全表快照 */
export async function createModel(
  req: CreateModelRequest,
): Promise<ModelRegistryResponse> {
  const { data } = await api.post<ModelRegistryResponse>('/models', req)
  return data
}

/**
 * 编辑白名单字段。被编辑的字段若属于同步管辖范围会**自动进入锁定**；
 * `unpin` 里的字段名则被解除锁定，重新跟随上游同步。
 */
export async function patchModel(
  upstreamId: string,
  patch: PatchModelRequest,
): Promise<ModelRegistryResponse> {
  const { data } = await api.patch<ModelRegistryResponse>(
    `/models/${encodeURIComponent(upstreamId)}`,
    patch,
  )
  return data
}

/** 删除一行。内置模型后端会拒绝 */
export async function deleteModel(
  upstreamId: string,
): Promise<ModelRegistryResponse> {
  const { data } = await api.delete<ModelRegistryResponse>(
    `/models/${encodeURIComponent(upstreamId)}`,
  )
  return data
}

/** 新增或覆盖一条别名 */
export async function upsertAlias(
  from: string,
  to: string,
): Promise<ModelRegistryResponse> {
  const { data } = await api.post<ModelRegistryResponse>('/models/aliases', {
    from,
    to,
  })
  return data
}

/** 删除一条别名。DELETE 带请求体 `{ from }` */
export async function deleteAlias(
  from: string,
): Promise<ModelRegistryResponse> {
  const { data } = await api.delete<ModelRegistryResponse>('/models/aliases', {
    data: { from },
  })
  return data
}

/** 更新同步设置。只返回设置本身，不返回全表 */
export async function setModelSyncSettings(
  req: SetModelSyncSettingsRequest,
): Promise<ModelRegistryResponse['settings']> {
  const { data } = await api.patch<ModelRegistryResponse['settings']>(
    '/models/settings',
    req,
  )
  return data
}
