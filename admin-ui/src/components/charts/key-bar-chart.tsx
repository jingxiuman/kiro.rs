import { memo, useMemo } from 'react'
import type { CSSProperties } from 'react'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend } from 'recharts'
import type { KeyDistribution } from '@/types/api'
import { tooltipContentStyle, tooltipCursorStyle } from './tooltip-style'
import { formatNumber } from '@/lib/utils'

/**
 * 按入口 Key 的堆叠柱。与 credential-bar-chart 同构，两处刻意不同：
 * - Tooltip 多一行 credits——本图表存在的理由就是回答「哪个 Key 吃掉了额度」
 * - 标签用 Key 名（已删除的 Key 后端回退成 `#id`），不做邮箱式截断
 */
const SERIES = [
  { key: 'inputTokens', name: '输入', color: '#3b82f6' },
  { key: 'outputTokens', name: '输出', color: '#10b981' },
  { key: 'cacheCreationTokens', name: '缓存写', color: '#f59e0b' },
  { key: 'cacheReadTokens', name: '缓存读', color: '#a855f7' },
] as const satisfies ReadonlyArray<{ key: keyof ChartDatum; name: string; color: string }>

interface Props {
  data: KeyDistribution[]
}

interface ChartDatum {
  cacheCreationTokens: number
  cacheReadTokens: number
  calls: number
  credits: number
  errors: number
  fullLabel: string
  inputTokens: number
  label: string
  outputTokens: number
  /** 四个 token 系列之和，即堆叠柱的总高度 */
  totalTokens: number
}

function KeyBarChartImpl({ data }: Props) {
  const formatted = useMemo(() => buildChartData(data), [data])

  if (data.length === 0) {
    return <EmptyKeyChart />
  }

  return <KeyChartContent data={formatted} />
}

function buildChartData(data: KeyDistribution[]): ChartDatum[] {
  return data.slice(0, 12).map((d) => {
    // keyId 0 是系统 Key，不是缺失值——标出来，否则面板上会被当成脏数据。
    const fullLabel = d.keyId === 0 ? `${d.keyName}（系统）` : d.keyName
    return {
      cacheCreationTokens: d.cacheCreationTokens,
      cacheReadTokens: d.cacheReadTokens,
      calls: d.calls,
      credits: d.credits,
      errors: d.errors,
      fullLabel,
      inputTokens: d.inputTokens,
      label: truncateLabel(fullLabel),
      outputTokens: d.outputTokens,
      totalTokens: d.inputTokens + d.outputTokens + d.cacheCreationTokens + d.cacheReadTokens,
    }
  })
}

function EmptyKeyChart() {
  return (
    <div className="flex h-[180px] items-center justify-center text-sm text-muted-foreground sm:h-[260px]">
      暂无数据
    </div>
  )
}

function KeyChartContent({ data }: { data: ChartDatum[] }) {
  return (
    <div className="h-[280px] sm:h-[340px]">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ top: 8, right: 8, left: -10, bottom: 52 }}>
          <CartesianGrid strokeDasharray="3 3" className="stroke-border/50" />
          <XAxis
            dataKey="label"
            tick={{ fontSize: 10 }}
            angle={-30}
            textAnchor="end"
            interval={0}
            height={64}
          />
          <YAxis tick={{ fontSize: 11 }} tickFormatter={(v: number) => formatNumber(v)} width={42} />
          <Tooltip content={<KeyTooltip />} cursor={tooltipCursorStyle} />
          <Legend verticalAlign="top" align="right" height={28} wrapperStyle={{ fontSize: 12 }} />
          {SERIES.map((s) => (
            <Bar key={s.key} dataKey={s.key} name={s.name} stackId="a" fill={s.color} isAnimationActive={false} />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

function KeyTooltip({
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
        <TooltipRow key={s.key} color={s.color} name={s.name} value={formatNumber(datum[s.key])} />
      ))}
      <TooltipRow name="合计" value={formatNumber(datum.totalTokens)} emphasized />
      <TooltipRow name="调用" value={formatNumber(datum.calls)} />
      <TooltipRow name="credits" value={datum.credits.toFixed(2)} emphasized />
      {datum.errors > 0 && <TooltipRow name="错误" value={formatNumber(datum.errors)} color="#ef4444" />}
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
  value: string
}) {
  return (
    <div style={emphasized ? TOOLTIP_TOTAL_ROW_STYLE : TOOLTIP_ROW_STYLE}>
      <span style={color ? { ...TOOLTIP_SWATCH_STYLE, background: color } : TOOLTIP_SWATCH_STYLE} />
      <span style={{ flex: 1 }}>{name}:</span>
      <span style={{ fontVariantNumeric: 'tabular-nums', fontWeight: emphasized ? 600 : undefined }}>
        {value}
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

export const KeyBarChart = memo(KeyBarChartImpl)

/** 仅用于 X 轴展示：Key 名可能很长，超过 16 字符截断 */
function truncateLabel(name: string): string {
  return name.length > 16 ? name.slice(0, 15) + '…' : name
}
