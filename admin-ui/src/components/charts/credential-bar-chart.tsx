import { memo, useMemo } from 'react'
import type { CSSProperties } from 'react'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts'
import type { CredentialDistribution } from '@/types/api'
import { tooltipContentStyle, tooltipCursorStyle } from './tooltip-style'
import { formatNumber } from '@/lib/utils'

/** 堆叠的四个 token 系列；柱子与 Tooltip 共用，避免两处漂移 */
const SERIES = [
  { key: 'inputTokens', name: '输入', color: '#3b82f6' },
  { key: 'outputTokens', name: '输出', color: '#10b981' },
  { key: 'cacheCreationTokens', name: '缓存写', color: '#f59e0b' },
  { key: 'cacheReadTokens', name: '缓存读', color: '#a855f7' },
] as const satisfies ReadonlyArray<{ key: keyof ChartDatum; name: string; color: string }>

interface Props {
  data: CredentialDistribution[]
}

interface ChartDatum {
  cacheCreationTokens: number
  cacheReadTokens: number
  calls: number
  errors: number
  fullLabel: string
  inputTokens: number
  label: string
  outputTokens: number
  /** 四个 token 系列之和，即堆叠柱的总高度 */
  totalTokens: number
}

function CredentialBarChartImpl({ data }: Props) {
  const formatted = useMemo(() => buildChartData(data), [data])

  if (data.length === 0) {
    return <EmptyCredentialChart />
  }

  return <CredentialChartContent data={formatted} />
}

function buildChartData(data: CredentialDistribution[]): ChartDatum[] {
  return data.slice(0, 12).map((d) => {
    const fullLabel = d.email ?? `#${d.credentialId}`
    return {
      cacheCreationTokens: d.cacheCreationTokens,
      cacheReadTokens: d.cacheReadTokens,
      calls: d.calls,
      errors: d.errors,
      fullLabel,
      inputTokens: d.inputTokens,
      label: d.email ? truncateEmail(d.email) : fullLabel,
      outputTokens: d.outputTokens,
      totalTokens: d.inputTokens + d.outputTokens + d.cacheCreationTokens + d.cacheReadTokens,
    }
  })
}

function EmptyCredentialChart() {
  return (
    <div className="flex h-[180px] items-center justify-center text-sm text-muted-foreground sm:h-[260px]">
      暂无数据
    </div>
  )
}

function CredentialChartContent({ data }: { data: ChartDatum[] }) {
  return (
    <div className="h-[280px] sm:h-[340px]">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ top: 8, right: 8, left: -10, bottom: 52 }}>
          {credentialChartAxes()}
          {credentialChartTooltip()}
          <Legend verticalAlign="top" align="right" height={28} wrapperStyle={{ fontSize: 12 }} />
          {credentialChartBars()}
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

function credentialChartAxes() {
  return [
    <CartesianGrid key="grid" strokeDasharray="3 3" className="stroke-border/50" />,
    <XAxis
      key="x"
      dataKey="label"
      tick={{ fontSize: 10 }}
      angle={-30}
      textAnchor="end"
      interval={0}
      height={64}
    />,
    <YAxis key="y" tick={{ fontSize: 11 }} tickFormatter={(v: number) => formatNumber(v)} width={42} />,
  ]
}

function credentialChartTooltip() {
  return <Tooltip content={<CredentialTooltip />} cursor={tooltipCursorStyle} />
}

function CredentialTooltip({
  active,
  payload,
  label,
}: {
  active?: boolean
  label?: string
  payload?: ReadonlyArray<{ payload?: ChartDatum }>
}) {
  const datum = payload?.[0]?.payload
  if (!active || !datum) return null
  return (
    <div style={{ ...tooltipContentStyle, minWidth: 200 }}>
      <div style={TOOLTIP_TITLE_STYLE}>{datum.fullLabel ?? label}</div>
      {SERIES.map((s) => (
        <TooltipRow key={s.key} color={s.color} name={s.name} value={datum[s.key]} />
      ))}
      <TooltipRow name="合计" value={datum.totalTokens} emphasized />
      <TooltipRow name="调用" value={datum.calls} />
      {datum.errors > 0 && <TooltipRow name="错误" value={datum.errors} color="#ef4444" />}
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
      <span style={{ fontVariantNumeric: 'tabular-nums', fontWeight: emphasized ? 600 : undefined }}>
        {formatNumber(value)}
      </span>
    </div>
  )
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

/** 合计行与上方明细之间加分隔线，避免被误读成第五个系列 */
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

function credentialChartBars() {
  return SERIES.map((s) => (
    <Bar key={s.key} dataKey={s.key} name={s.name} stackId="a" fill={s.color} isAnimationActive={false} />
  ))
}

export const CredentialBarChart = memo(CredentialBarChartImpl)

/** 仅用于 X 轴展示：保留 @ 后域名前 1-2 段，整体最长 22 字符 */
function truncateEmail(email: string): string {
  if (email.length <= 22) return email
  const at = email.indexOf('@')
  if (at < 0) return email.slice(0, 20) + '…'
  const name = email.slice(0, at)
  const domain = email.slice(at + 1)
  const shortName = name.length > 12 ? name.slice(0, 11) + '…' : name
  return `${shortName}@${domain}`
}
