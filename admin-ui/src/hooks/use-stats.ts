import { keepPreviousData, useQuery } from '@tanstack/react-query'
import {
  getBalanceHistory,
  getByCredential,
  getByModel,
  getCreditsByCredential,
  getOverview,
  getTimeSeries,
} from '@/api/stats'
import type { StatsFilter, StatsTimeFilter } from '@/types/api'

/**
 * 统计接口共用配置
 *
 * - `staleTime: 25_000`：30s 自动刷新前不再触发后台 refetch（防止跨 Tab 切换抖动）
 * - `placeholderData: keepPreviousData`：切换 range 或 tab 期间保留上次数据，
 *   chart 组件输入引用稳定 → 不会卸载重挂
 * - `refetchOnWindowFocus: false`：Admin 面板长时间挂着时减少瞬时压力
 */
const COMMON = {
  refetchInterval: 30_000,
  staleTime: 25_000,
  placeholderData: keepPreviousData,
  refetchOnWindowFocus: false,
} as const

export function useOverview() {
  return useQuery({
    queryKey: ['stats', 'overview'],
    queryFn: getOverview,
    ...COMMON,
  })
}

function timeKey(time: StatsTimeFilter) {
  return [
    time.range ?? 'custom',
    time.startDate ?? '',
    time.endDate ?? '',
    time.granularity,
  ] as const
}

export function useTimeSeries(time: StatsTimeFilter, filter?: StatsFilter) {
  return useQuery({
    queryKey: ['stats', 'timeseries', ...timeKey(time), filter?.keyId ?? 'all', filter?.group ?? 'all'],
    queryFn: () => getTimeSeries(time, filter),
    ...COMMON,
  })
}

export function useByModel(time: StatsTimeFilter, filter?: StatsFilter) {
  return useQuery({
    queryKey: ['stats', 'by-model', ...timeKey(time), filter?.keyId ?? 'all', filter?.group ?? 'all'],
    queryFn: () => getByModel(time, filter),
    ...COMMON,
  })
}

export function useByCredential(time: StatsTimeFilter, filter?: StatsFilter) {
  return useQuery({
    queryKey: ['stats', 'by-credential', ...timeKey(time), filter?.keyId ?? 'all', filter?.group ?? 'all'],
    queryFn: () => getByCredential(time, filter),
    ...COMMON,
  })
}

export function useCreditsByCredential(time: StatsTimeFilter, filter?: StatsFilter) {
  return useQuery({
    queryKey: [
      'stats',
      'credits-by-credential',
      ...timeKey(time),
      filter?.keyId ?? 'all',
      filter?.group ?? 'all',
    ],
    queryFn: () => getCreditsByCredential(time, filter),
    ...COMMON,
  })
}

/**
 * 余额历史与消耗速率。
 *
 * 刷新间隔用 COMMON 的 30s 即可——后端快照本身 5 分钟才产生一个新点，
 * 更频繁的拉取不会带来新信息，但保持与其他统计一致的手感。
 *
 * `credentialId = null` 表示「全部账号」（返回各账号的速率，points 含所有账号的点），
 * 这是仪表盘的用法；传具体 id 则只看单账号。用 `enabled` 控制是否发请求。
 */
export function useBalanceHistory(
  hours: number,
  credentialId: number | null,
  enabled = true,
) {
  return useQuery({
    queryKey: ['stats', 'balance-history', hours, credentialId ?? 'all'],
    queryFn: () => getBalanceHistory(hours, credentialId ?? undefined),
    enabled,
    ...COMMON,
  })
}
