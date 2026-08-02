import axios from 'axios'
import { storage } from '@/lib/storage'
import type {
  BalanceHistory,
  CredentialDistribution,
  CreditsByCredential,
  ModelDistribution,
  OverviewStats,
  StatsFilter,
  StatsTimeFilter,
  TimeSeriesPoint,
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

export async function getOverview(): Promise<OverviewStats> {
  const { data } = await api.get<OverviewStats>('/stats/overview')
  return data
}

function statsParams(time: StatsTimeFilter, filter?: StatsFilter) {
  return {
    ...time,
    ...(filter?.keyId !== undefined ? { keyId: filter.keyId } : {}),
    ...(filter?.group ? { group: filter.group } : {}),
  }
}

export async function getTimeSeries(time: StatsTimeFilter, filter?: StatsFilter): Promise<TimeSeriesPoint[]> {
  const { data } = await api.get<TimeSeriesPoint[]>('/stats/timeseries', {
    params: statsParams(time, filter),
  })
  return data
}

export async function getByModel(time: StatsTimeFilter, filter?: StatsFilter): Promise<ModelDistribution[]> {
  const { data } = await api.get<ModelDistribution[]>('/stats/by-model', {
    params: statsParams(time, filter),
  })
  return data
}

export async function getByCredential(time: StatsTimeFilter, filter?: StatsFilter): Promise<CredentialDistribution[]> {
  const { data } = await api.get<CredentialDistribution[]>('/stats/by-credential', {
    params: statsParams(time, filter),
  })
  return data
}

export async function getCreditsByCredential(
  time: StatsTimeFilter,
  filter?: StatsFilter,
): Promise<CreditsByCredential> {
  const { data } = await api.get<CreditsByCredential>('/stats/credits-by-credential', {
    params: statsParams(time, filter),
  })
  return data
}

/**
 * 余额历史与消耗速率。
 * credentialId 省略时返回全部账号（前端按 credentialId 分组画多条线）。
 */
export async function getBalanceHistory(
  hours: number,
  credentialId?: number,
): Promise<BalanceHistory> {
  const { data } = await api.get<BalanceHistory>('/stats/balance-history', {
    params: { hours, ...(credentialId !== undefined ? { credentialId } : {}) },
  })
  return data
}
