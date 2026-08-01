import { keepPreviousData, useQuery } from '@tanstack/react-query'
import {
  getOpsCredentials,
  getOpsErrorCrosstab,
  getOpsErrorFingerprints,
  getOpsEvents,
  getOpsOverview,
  getOpsProxies,
  getOpsRetryEffectiveness,
  getOpsTrend,
  getPhaseBaseline,
} from '@/api/ops'
import type { CrosstabDimension } from '@/types/api'

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

export function useOpsErrorCrosstab(hours: number, dim: CrosstabDimension) {
  return useQuery({
    queryKey: ['ops', 'error-crosstab', hours, dim],
    queryFn: () => getOpsErrorCrosstab(hours, dim),
    ...COMMON,
  })
}

export function useOpsErrorFingerprints(hours: number, limit = 50) {
  return useQuery({
    queryKey: ['ops', 'error-fingerprints', hours, limit],
    queryFn: () => getOpsErrorFingerprints(hours, limit),
    ...COMMON,
  })
}

export function useOpsRetryEffectiveness(hours: number) {
  return useQuery({
    queryKey: ['ops', 'retry-effectiveness', hours],
    queryFn: () => getOpsRetryEffectiveness(hours),
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

export function usePhaseBaseline(hours = 24) {
  return useQuery({
    queryKey: ['ops', 'phase-baseline', hours],
    queryFn: () => getPhaseBaseline(hours),
    ...COMMON,
  })
}
