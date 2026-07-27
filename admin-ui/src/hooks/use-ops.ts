import { keepPreviousData, useQuery } from '@tanstack/react-query'
import {
  getOpsCredentials,
  getOpsEvents,
  getOpsOverview,
  getOpsProxies,
  getOpsTrend,
} from '@/api/ops'

/** 与 use-stats 同样的刷新策略：30s 轮询、切窗口保留旧数据 */
const COMMON = {
  refetchInterval: 30_000,
  staleTime: 25_000,
  placeholderData: keepPreviousData,
  refetchOnWindowFocus: false,
} as const

export function useOpsOverview(hours: number) {
  return useQuery({
    queryKey: ['ops', 'overview', hours],
    queryFn: () => getOpsOverview(hours),
    ...COMMON,
  })
}

export function useOpsTrend(hours: number) {
  return useQuery({
    queryKey: ['ops', 'trend', hours],
    queryFn: () => getOpsTrend(hours),
    ...COMMON,
  })
}

export function useOpsCredentials(hours: number) {
  return useQuery({
    queryKey: ['ops', 'credentials', hours],
    queryFn: () => getOpsCredentials(hours),
    ...COMMON,
  })
}

export function useOpsProxies(hours: number) {
  return useQuery({
    queryKey: ['ops', 'proxies', hours],
    queryFn: () => getOpsProxies(hours),
    ...COMMON,
  })
}

export function useOpsEvents(limit = 100) {
  return useQuery({
    queryKey: ['ops', 'events', limit],
    queryFn: () => getOpsEvents(limit),
    ...COMMON,
  })
}
