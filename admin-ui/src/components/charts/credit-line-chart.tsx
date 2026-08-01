import { memo, useMemo } from 'react'
import type { CSSProperties } from 'react'
import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { CreditsByCredential, StatsGranularity } from '@/types/api'
import { tooltipCursorStyle } from './tooltip-style'
import { formatCredits } from '@/lib/utils'

interface Props {
  data?: CreditsByCredential
  granularity: StatsGranularity
}

/**
 * 账号系列配色。10 色对应后端默认 Top 10，超出则循环复用——
 * 复用会让两条线同色，但截断提示已告知用户「还有更多账号」，
 * 这里不再为极端情况引入色相计算。
 */
const SERIES_COLORS = [
  '#3b82f6',
  '#10b981',
  '#f59e0b',
  '#ec4899',
  '#8b5cf6',
  '#06b6d4',
  '#ef4444',
  '#84cc16',
  '#f97316',
  '#a855f7',
] as const

/** recharts 需要宽表：每个时间点一行，每个账号一列 */
interface ChartRow {
  label: string
  /** dataKey 形如 `c5`（credentialId=5）；不用纯数字，避免与 recharts 的索引语义混淆 */
  [seriesKey: string]: string | number
}

interface SeriesDef {
  color: string
  key: string
  name: string
  totalCredits: number
}

function seriesKey(credentialId: number): string {
  return `c${credentialId}`
}

function formatTs(ts: string, granularity: StatsGranularity): string {
  const d = new Date(ts)
  const md = `${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
  if (granularity === 'day') return `${d.getFullYear()}-${md}`
  return `${md} ${String(d.getHours()).padStart(2, '0')}:00`
}

/** 仅用于图例展示：邮箱过长时压缩本地部分，保留域名 */
function shortLabel(email: string | null, credentialId: number): string {
  if (!email) return `#${credentialId}`
  if (email.length <= 22) return email
  const at = email.indexOf('@')
  if (at < 0) return `${email.slice(0, 20)}…`
  const name = email.slice(0, at)
  const domain = email.slice(at + 1)
  return `${name.length > 12 ? `${name.slice(0, 11)}…` : name}@${domain}`
}

function buildSeries(data: CreditsByCredential): SeriesDef[] {
  return data.series.map((s, i) => ({
    color: SERIES_COLORS[i % SERIES_COLORS.length],
    key: seriesKey(s.credentialId),
    name: shortLabel(s.email, s.credentialId),
    totalCredits: s.totalCredits,
  }))
}

/**
 * pivot 成宽表。缺失的账号补 0 而非留空：积分是流量型指标，
 * 某小时没消耗就是 0，留空会让 recharts 断线、看着像「数据缺失」。
 */
function buildRows(data: CreditsByCredential, granularity: StatsGranularity): ChartRow[] {
  const keys = data.series.map((s) => s.credentialId)
  return data.points.map((p) => {
    const row: ChartRow = { label: formatTs(p.ts, granularity) }
    for (const id of keys) {
      row[seriesKey(id)] = p.credits[String(id)] ?? 0
    }
    return row
  })
}

function pickXAxisInterval(len: number): number | 'preserveStartEnd' {
  if (len <= 12) return 0
  if (len <= 48) return Math.ceil(len / 12)
  return Math.ceil(len / 16)
}

function CreditLineChartImpl({ data, granularity }: Props) {
  const series = useMemo(() => (data ? buildSeries(data) : []), [data])
  const rows = useMemo(() => (data ? buildRows(data, granularity) : []), [data, granularity])
  const interval = useMemo(() => pickXAxisInterval(rows.length), [rows.length])

  if (series.length === 0) {
    return <EmptyCreditChart />
  }

  return (
    <div className="h-[260px] sm:h-[320px]">
      <ResponsiveContainer width="100%" height="100%">
        {/* right 留 30：X 轴最后一个刻度标签（形如 07-31 06:00）否则会被右边界裁掉 */}
        <LineChart data={rows} margin={{ top: 16, right: 30, left: -6, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" className="stroke-border/50" />
          <XAxis
            dataKey="label"
            tick={{ fontSize: 11 }}
            className="fill-muted-foreground"
            interval={interval}
          />
          <YAxis
            tick={{ fontSize: 11 }}
            className="fill-muted-foreground"
            tickFormatter={(v: number) => formatCredits(v)}
            width={56}
          />
          <Tooltip content={<CreditTooltip series={series} />} cursor={tooltipCursorStyle} />
          <Legend verticalAlign="top" align="center" iconType="circle" wrapperStyle={LEGEND_STYLE} />
          {series.map((s) => (
            <Line
              key={s.key}
              type="monotone"
              dataKey={s.key}
              name={s.name}
              stroke={s.color}
              dot={false}
              strokeWidth={2}
              isAnimationActive
              animationDuration={550}
              animationEasing="ease-out"
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}

function EmptyCreditChart() {
  return (
    <div className="flex h-[260px] items-center justify-center text-sm text-muted-foreground sm:h-[320px]">
      该时间范围内没有积分消耗
    </div>
  )
}

/**
 * Tooltip 只列当前桶有消耗的账号，并按积分降序。
 * 全列 10 条（含一堆 0）会把真正在烧的账号埋掉。
 */
function CreditTooltip({
  active,
  label,
  payload,
  series,
}: {
  active?: boolean
  label?: string
  payload?: ReadonlyArray<{ dataKey?: string | number; value?: number }>
  series: SeriesDef[]
}) {
  if (!active || !payload?.length) return null
  const colorOf = new Map(series.map((s) => [s.key, s.color]))
  const nameOf = new Map(series.map((s) => [s.key, s.name]))
  const rows = payload
    .filter(
      (p): p is { dataKey: string; value: number } =>
        typeof p.dataKey === 'string' && typeof p.value === 'number' && p.value > 0,
    )
    .sort((a, b) => b.value - a.value)
  const total = rows.reduce((sum, r) => sum + r.value, 0)

  return (
    <div style={TOOLTIP_STYLE}>
      <div style={TOOLTIP_TITLE_STYLE}>{label}</div>
      {rows.length === 0 ? (
        <div style={{ color: 'rgba(255,255,255,0.66)' }}>无消耗</div>
      ) : (
        <>
          {rows.map((r) => (
            <TooltipRow
              key={r.dataKey}
              color={colorOf.get(r.dataKey)}
              name={nameOf.get(r.dataKey) ?? r.dataKey}
              value={r.value}
            />
          ))}
          {rows.length > 1 && <TooltipRow emphasized name="合计" value={total} />}
        </>
      )}
    </div>
  )
}

function TooltipRow({
  color,
  emphasized,
  name,
  value,
}: {
  color?: string
  emphasized?: boolean
  name: string
  value: number
}) {
  return (
    <div style={emphasized ? TOOLTIP_TOTAL_ROW_STYLE : TOOLTIP_ROW_STYLE}>
      <span style={color ? { ...TOOLTIP_SWATCH_STYLE, background: color } : TOOLTIP_SWATCH_STYLE} />
      <span style={{ flex: 1 }}>{name}:</span>
      <span
        style={{ fontVariantNumeric: 'tabular-nums', fontWeight: emphasized ? 600 : undefined }}
      >
        {formatCredits(value)}
      </span>
    </div>
  )
}

const LEGEND_STYLE: CSSProperties = {
  fontSize: 11,
  paddingBottom: 8,
}

const TOOLTIP_STYLE: CSSProperties = {
  background: 'rgba(20,20,20,0.94)',
  border: '1px solid rgba(255,255,255,0.08)',
  borderRadius: 10,
  boxShadow: '0 8px 24px rgba(0,0,0,0.25)',
  color: '#fff',
  fontSize: 12,
  maxWidth: 300,
  minWidth: 200,
  padding: '10px 14px',
}

const TOOLTIP_TITLE_STYLE: CSSProperties = {
  color: 'rgba(255,255,255,0.92)',
  fontWeight: 600,
  marginBottom: 6,
}

const TOOLTIP_ROW_STYLE: CSSProperties = {
  alignItems: 'center',
  display: 'flex',
  gap: 8,
  padding: '2px 0',
}

const TOOLTIP_TOTAL_ROW_STYLE: CSSProperties = {
  ...TOOLTIP_ROW_STYLE,
  borderTop: '1px solid rgba(255,255,255,0.14)',
  marginTop: 4,
  paddingTop: 5,
}

const TOOLTIP_SWATCH_STYLE: CSSProperties = {
  borderRadius: 2,
  display: 'inline-block',
  height: 10,
  width: 10,
}

export const CreditLineChart = memo(CreditLineChartImpl)
