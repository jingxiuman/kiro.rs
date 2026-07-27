import axios from 'axios'
import { storage } from '@/lib/storage'
import type {
  OpsCredentialRow,
  OpsEvent,
  OpsOverview,
  OpsProxiesResponse,
  OpsTrendPoint,
  PhaseBaselineRow,
} from '@/types/api'

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

export async function getOpsOverview(hours: number): Promise<OpsOverview> {
  const { data } = await api.get<OpsOverview>('/ops/overview', { params: { hours } })
  return data
}

export async function getOpsTrend(hours: number): Promise<OpsTrendPoint[]> {
  const { data } = await api.get<OpsTrendPoint[]>('/ops/trend', { params: { hours } })
  return data
}

export async function getOpsCredentials(hours: number): Promise<OpsCredentialRow[]> {
  const { data } = await api.get<OpsCredentialRow[]>('/ops/credentials', { params: { hours } })
  return data
}

export async function getOpsProxies(hours: number): Promise<OpsProxiesResponse> {
  const { data } = await api.get<OpsProxiesResponse>('/ops/proxies', { params: { hours } })
  return data
}

export async function getOpsEvents(limit = 100): Promise<OpsEvent[]> {
  const { data } = await api.get<OpsEvent[]>('/ops/events', { params: { limit } })
  return data
}

export async function getPhaseBaseline(hours = 24): Promise<PhaseBaselineRow[]> {
  const { data } = await api.get<PhaseBaselineRow[]>('/ops/phase-baseline', {
    params: { hours },
  })
  return data
}
