import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  createModel,
  deleteAlias,
  deleteModel,
  getModelRegistry,
  patchModel,
  setModelSyncSettings,
  syncModels,
  upsertAlias,
} from '@/api/models'
import type {
  CreateModelRequest,
  ModelRegistryResponse,
  PatchModelRequest,
  SetModelSyncSettingsRequest,
} from '@/types/api'

export const MODEL_REGISTRY_KEY = ['model-registry'] as const

/**
 * 模型注册表全表查询。
 *
 * 与凭据列表不同，这里**不做轮询**：模型表只在人工编辑或手动/定时同步后变化，
 * 定时轮询只会在用户编辑输入框时把内容刷掉。
 */
export function useModelRegistry(enabled = true) {
  return useQuery({
    queryKey: MODEL_REGISTRY_KEY,
    queryFn: getModelRegistry,
    enabled,
  })
}

/**
 * 所有写端点的公共收尾：后端返回的就是改动后的全表快照，直接写进缓存，
 * 省掉一次 GET，也避免「保存后短暂显示旧值」。
 */
function useRegistryMutation<TVars>(
  fn: (vars: TVars) => Promise<ModelRegistryResponse>,
) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: fn,
    onSuccess: (data) => {
      queryClient.setQueryData(MODEL_REGISTRY_KEY, data)
    },
  })
}

export function usePatchModel() {
  return useRegistryMutation(
    ({ upstreamId, patch }: { upstreamId: string; patch: PatchModelRequest }) =>
      patchModel(upstreamId, patch),
  )
}

export function useCreateModel() {
  return useRegistryMutation((req: CreateModelRequest) => createModel(req))
}

export function useDeleteModel() {
  return useRegistryMutation((upstreamId: string) => deleteModel(upstreamId))
}

export function useUpsertAlias() {
  return useRegistryMutation(({ from, to }: { from: string; to: string }) =>
    upsertAlias(from, to),
  )
}

export function useDeleteAlias() {
  return useRegistryMutation((from: string) => deleteAlias(from))
}

/**
 * 手动同步。返回的是 diff 摘要而非全表，所以这里只能失效查询触发重新拉取。
 *
 * `mutate(true)` 对应强制放行一次消失判定（`?force=true`），供护栏横幅上的
 * 「确认并强制执行」按钮使用；不传或传 `false` 就是普通的「立即同步」。
 */
export function useSyncModels() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (force: boolean) => syncModels(force),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: MODEL_REGISTRY_KEY })
    },
  })
}

/**
 * 更新同步设置。端点只回 settings，就地补进缓存中的全表快照。
 */
export function useSetModelSyncSettings() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (req: SetModelSyncSettingsRequest) => setModelSyncSettings(req),
    onSuccess: (settings) => {
      queryClient.setQueryData<ModelRegistryResponse>(
        MODEL_REGISTRY_KEY,
        (prev) => (prev ? { ...prev, settings } : prev),
      )
    },
  })
}
