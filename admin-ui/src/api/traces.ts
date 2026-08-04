import axios from 'axios'
import { storage } from '@/lib/storage'
import type { FailureStatsMap, TracePage, TraceQuery } from '@/types/api'

const api = axios.create({
  baseURL: '/api/admin',
  timeout: 15000,
  headers: { 'Content-Type': 'application/json' },
})

api.interceptors.request.use((config) => {
  const apiKey = storage.getApiKey()
  if (apiKey) config.headers['x-api-key'] = apiKey
  return config
})

export async function getTraces(query: TraceQuery): Promise<TracePage> {
  const params: Record<string, string> = {}
  if (query.operation) params.operation = query.operation
  if (query.status) params.status = query.status
  if (query.errorType) params.errorType = query.errorType
  if (query.credentialId != null) params.credentialId = String(query.credentialId)
  if (query.keyId != null) params.keyId = String(query.keyId)
  if (query.failedAttemptCredentialId != null)
    params.failedAttemptCredentialId = String(query.failedAttemptCredentialId)
  if (query.model) params.model = query.model
  if (query.group) params.group = query.group
  if (query.onlyFailed) params.onlyFailed = 'true'
  if (query.limit != null) params.limit = String(query.limit)
  if (query.offset != null) params.offset = String(query.offset)
  const { data } = await api.get<TracePage>('/traces', { params })
  return data
}

/**
 * 读取某条 trace 的原始入站请求体（storeRequestBodies=true 时才有数据）。
 * 404 = 未启用保留 / 已过期 / trace 不存在——由调用方按 axios 错误的 status 区分提示。
 */
export async function getTraceRequestBody(traceId: string): Promise<unknown> {
  const { data } = await api.get<unknown>(
    `/traces/${encodeURIComponent(traceId)}/request-body`,
  )
  return data
}

export async function getFailureStats(): Promise<FailureStatsMap> {
  const { data } = await api.get<FailureStatsMap>('/traces/failure-stats')
  return data
}
