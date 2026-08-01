import { useMemo, useState } from 'react'
import { Card, CardContent } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import {
  Activity,
  AlertTriangle,
  Network,
  ScissorsLineDashed,
  ShieldAlert,
  Timer,
} from 'lucide-react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import {
  useOpsCredentials,
  useOpsErrorCrosstab,
  useOpsErrorFingerprints,
  useOpsEvents,
  useOpsOverview,
  useOpsProxies,
  useOpsRetryEffectiveness,
  useOpsTrend,
} from '@/hooks/use-ops'
import { useTraces } from '@/hooks/use-traces'
import type {
  CrosstabBucket,
  CrosstabDimension,
  CrosstabRow,
  DurationPercentiles,
  ErrorFingerprint,
  OpsCredentialRow,
  OpsEvent,
  OpsProxyRow,
  ProxyPoolEntry,
  RetryEffectiveness,
  RetryLadderStep,
  TraceRecord,
} from '@/types/api'
import { cn, formatNumber } from '@/lib/utils'

const WINDOWS: { label: string; hours: number }[] = [
  { label: '1 小时', hours: 1 },
  { label: '24 小时', hours: 24 },
  { label: '7 天', hours: 168 },
]

const ERROR_TYPE_LABELS: Record<string, string> = {
  quota_exhausted: '额度耗尽',
  account_throttled: '账号风控',
  auth_failed: '鉴权失败',
  transient: '瞬态错误',
  network_error: '网络错误',
  bad_request: '请求错误',
  stream_interrupted: '流中断',
  upstream_truncated: '上游截断',
  upstream_invalid: '上游非法JSON',
  unknown: '未知',
}

const EVENT_CATEGORY_LABELS: Record<string, string> = {
  proxy_auto_disable: '代理自动禁用（请求级）',
  proxy_probe_disable: '代理自动禁用（探测）',
  proxy_reassign: '凭据换绑',
}

function formatDuration(ms?: number | null): string {
  if (ms == null) return '—'
  if (ms < 1000) return `${ms}ms`
  return `${(ms / 1000).toFixed(1)}s`
}

/**
 * 中断耗时卡的副标题。
 *
 * 头条取 p95 而非 p99：实测中断样本 7 天仅约 20 条，n 这么小时 p99 就等于最大值
 * 本身，拿它当头条会让人以为有分位精度。同时把 p50 与 n 一起显示 —— p50 与 p95
 * 拉开（如 240s vs 720s）就是"链路上存在多个不同固定超时"的信号。
 */
function interruptedDurationSub(d?: DurationPercentiles | null): string {
  if (!d) return '窗口内无中断'
  return `p50 ${formatDuration(d.p50)} · p99 ${formatDuration(d.p99)} · n=${d.n}`
}

function pct(part: number, total: number): string {
  if (total === 0) return '—'
  return `${((part / total) * 100).toFixed(1)}%`
}

/** 概览统计卡 */
function StatCard({
  icon,
  label,
  value,
  sub,
  tone,
}: {
  icon: React.ReactNode
  label: string
  value: string
  sub?: string
  tone?: 'ok' | 'warn' | 'bad'
}) {
  return (
    <Card>
      <CardContent className="flex items-center gap-3 p-4">
        <div
          className={cn(
            'flex h-9 w-9 shrink-0 items-center justify-center rounded-lg',
            tone === 'bad'
              ? 'bg-destructive/10 text-destructive'
              : tone === 'warn'
                ? 'bg-amber-500/10 text-amber-500'
                : 'bg-primary/10 text-primary',
          )}
        >
          {icon}
        </div>
        <div className="min-w-0">
          <div className="text-xs text-muted-foreground">{label}</div>
          <div className="truncate text-lg font-semibold leading-tight">{value}</div>
          {sub ? <div className="truncate text-[11px] text-muted-foreground">{sub}</div> : null}
        </div>
      </CardContent>
    </Card>
  )
}

/** 错误类型的固定配色。未列出的类型回落到灰色，不至于因为新增类型而无色 */
const ERROR_TYPE_COLORS: Record<string, string> = {
  transient: '#f59e0b',
  network_error: '#ef4444',
  stream_interrupted: '#a855f7',
  upstream_truncated: '#ec4899',
  bad_request: '#3b82f6',
  client_disconnected: '#64748b',
  unknown: '#94a3b8',
}
const ERROR_TYPE_FALLBACK = '#94a3b8'

function formatBucketTime(epoch: number): string {
  return new Date(epoch * 1000).toLocaleString('zh-CN', {
    month: 'numeric',
    day: 'numeric',
    hour: 'numeric',
  })
}

const TREND_TOOLTIP_STYLE = {
  background: 'hsl(var(--card))',
  border: '1px solid hsl(var(--border))',
  borderRadius: 8,
  fontSize: 12,
} as const

/**
 * 按小时趋势。两种视角：
 * - 按状态：成功/错误/中断，回答「总量与失败量」
 * - 按错误类型：只堆叠错误，回答「涨的是哪一类」——这是 8e7cd59 加 byErrorType
 *   的目的，突发事件与基线噪声在「按状态」视图里混在一根柱子里分不开
 */
function TrendChart({ hours }: { hours: number }) {
  const [mode, setMode] = useState<'status' | 'errorType'>('status')
  const { data } = useOpsTrend(hours)
  const points = useMemo(() => data ?? [], [data])

  // 窗口内出现过的错误类型（决定要画几个 Bar），按总量降序让主要类型在底部
  const errorTypes = useMemo(() => {
    const totals = new Map<string, number>()
    for (const p of points) {
      for (const [k, v] of Object.entries(p.byErrorType ?? {})) {
        totals.set(k, (totals.get(k) ?? 0) + v)
      }
    }
    return [...totals.entries()].sort((a, b) => b[1] - a[1]).map(([k]) => k)
  }, [points])

  const chartData = useMemo(
    () =>
      points.map((p) => {
        const row: Record<string, string | number> = { time: formatBucketTime(p.bucketEpoch) }
        if (mode === 'status') {
          row['成功'] = p.success
          row['错误'] = p.error
          row['中断'] = p.interrupted
        } else {
          // 缺失的类型补 0：Bar 的 dataKey 固定，缺键会让该段断开
          for (const t of errorTypes) row[t] = p.byErrorType?.[t] ?? 0
        }
        return row
      }),
    [points, mode, errorTypes],
  )

  const noErrors = mode === 'errorType' && errorTypes.length === 0

  return (
    <Card>
      <CardContent className="p-4">
        <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
          <div className="text-sm font-medium">请求趋势（按小时）</div>
          <div className="flex items-center gap-1 rounded-full border border-border/60 p-0.5">
            {(
              [
                { k: 'status', label: '按状态' },
                { k: 'errorType', label: '按错误类型' },
              ] as const
            ).map((m) => (
              <Button
                key={m.k}
                size="sm"
                variant={mode === m.k ? 'default' : 'ghost'}
                className="h-7 rounded-full px-3 text-xs"
                onClick={() => setMode(m.k)}
              >
                {m.label}
              </Button>
            ))}
          </div>
        </div>
        {mode === 'errorType' && !noErrors && (
          <p className="mb-2 text-[11px] text-muted-foreground">
            仅堆叠错误请求（含中断类）。各段之和为「错误+中断」，与「按状态」视图的
            错误段不等 —— 两者口径不同。
          </p>
        )}
        <div className="h-56">
          {noErrors ? (
            <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
              窗口内没有错误
            </div>
          ) : (
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={chartData} margin={{ top: 4, right: 8, left: -16, bottom: 0 }}>
                <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.25} />
                <XAxis dataKey="time" tick={{ fontSize: 11 }} />
                <YAxis tick={{ fontSize: 11 }} allowDecimals={false} />
                <Tooltip contentStyle={TREND_TOOLTIP_STYLE} />
                <Legend wrapperStyle={{ fontSize: 12 }} />
                {mode === 'status' ? (
                  [
                    <Bar key="s" dataKey="成功" stackId="a" fill="#22c55e" />,
                    <Bar key="e" dataKey="错误" stackId="a" fill="#ef4444" />,
                    <Bar key="i" dataKey="中断" stackId="a" fill="#f59e0b" />,
                  ]
                ) : (
                  errorTypes.map((t) => (
                    <Bar
                      key={t}
                      dataKey={t}
                      stackId="a"
                      fill={ERROR_TYPE_COLORS[t] ?? ERROR_TYPE_FALLBACK}
                    />
                  ))
                )}
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      </CardContent>
    </Card>
  )
}

/** 错误类型分布列表 */
function ErrorTypeList({ hours }: { hours: number }) {
  const { data } = useOpsOverview(hours)
  const items = data?.byErrorType ?? []
  const max = items.reduce((m, i) => Math.max(m, i.count), 0)
  return (
    <Card>
      <CardContent className="p-4">
        <div className="mb-3 text-sm font-medium">错误类型分布</div>
        {items.length === 0 ? (
          <div className="py-6 text-center text-sm text-muted-foreground">窗口内无错误</div>
        ) : (
          <div className="space-y-2">
            {items.map((i) => (
              <div key={i.errorType} className="flex items-center gap-2 text-[13px]">
                <span className="w-20 shrink-0 text-muted-foreground">
                  {ERROR_TYPE_LABELS[i.errorType] ?? i.errorType}
                </span>
                <div className="h-2 flex-1 overflow-hidden rounded-full bg-secondary">
                  <div
                    className={cn(
                      'h-full rounded-full',
                      i.errorType === 'stream_interrupted' || i.errorType === 'upstream_truncated'
                        ? 'bg-amber-500'
                        : 'bg-destructive/80',
                    )}
                    style={{ width: `${max > 0 ? (i.count / max) * 100 : 0}%` }}
                  />
                </div>
                <span className="w-10 shrink-0 text-right font-mono">{formatNumber(i.count)}</span>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  )
}

const CROSSTAB_DIMS: { label: string; value: CrosstabDimension }[] = [
  { label: '按凭据', value: 'credential' },
  { label: '按代理', value: 'proxy' },
  { label: '按模型', value: 'model' },
  { label: '按端点', value: 'endpoint' },
]

/** lift 判读阈值。≥2 视为超额，<1.25 视为与流量相称，中间为灰区 */
const LIFT_HIGH = 2.0
const LIFT_NORMAL = 1.25
/**
 * lift 可信所需的最小流量（分母）。低于此值时倍数由极小的分母算出，不可靠。
 */
const LIFT_MIN_TRAFFIC = 30
/**
 * lift 可信所需的最小错误数（分子）。
 *
 * 分母守卫不够：实测有过「1 个错误 / 1.1K 流量」算出 lift 3.17 被标红的情形，
 * 流量足够大所以分母守卫放行了，但 1 个错误无论分母多大都不构成模式。
 * 分子分母任一过小就不着色，只如实显示数值，避免把噪声渲染成告警。
 */
const LIFT_MIN_ERRORS = 3

/**
 * 桶标签。空串在不同维度含义不同，必须按维度分别解释：
 * proxy 维度的空串 = 出口未知（该列存在前的历史行）；
 * endpoint 维度的空串 = 请求没走到上游，压根没有端点可记（实测这类行全属凭据 0）。
 * 一律显示成「直连/未知」会把后者说成一个不存在的事实。
 */
function bucketLabel(b: CrosstabBucket, dim: CrosstabDimension): string {
  if (b.email) return b.email
  if (b.key === '') {
    return dim === 'endpoint' ? '(未到达上游)' : '(直连/未知)'
  }
  if (b.key === 'direct') return '直连'
  return b.key
}

/**
 * error_type × 维度 交叉表面板。
 *
 * 面板的判读重点是 lift 而非集中度：流量分布不均时，占流量最大的对象在每种错误上
 * 都会显得「集中」。实测 claude-opus-5 占全流量 63%，集中度 0.97 看着像元凶，
 * 但 lift 仅 1.53 —— 与流量相称。所以 lift 用色彩强调，集中度只作为副信息。
 */
function ErrorCrosstabPanel({ hours }: { hours: number }) {
  const [dim, setDim] = useState<CrosstabDimension>('credential')
  const { data } = useOpsErrorCrosstab(hours, dim)
  const rows = data?.rows ?? []

  return (
    <Card>
      <CardContent className="p-4 sm:p-5">
        <div className="mb-3 flex flex-col gap-2 sm:flex-row sm:items-start sm:justify-between">
          <div>
            <h2 className="text-base font-semibold tracking-tight">错误交叉分析</h2>
            <p className="text-[12px] text-muted-foreground">
              看 lift 而非集中度：lift≈1 说明错误份额与流量份额相称（不是问题），
              明显 &gt;1 才是错得不成比例
            </p>
          </div>
          <div className="flex items-center gap-1 rounded-full border border-border/60 p-0.5">
            {CROSSTAB_DIMS.map((d) => (
              <Button
                key={d.value}
                size="sm"
                variant={dim === d.value ? 'default' : 'ghost'}
                className="h-7 rounded-full px-3 text-xs"
                onClick={() => setDim(d.value)}
              >
                {d.label}
              </Button>
            ))}
          </div>
        </div>
        {rows.length === 0 ? (
          <div className="py-8 text-center text-sm text-muted-foreground">窗口内无错误</div>
        ) : (
          <div className="space-y-3">
            {rows.map((r) => (
              <CrosstabRowBlock key={r.errorType} row={r} dim={dim} />
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  )
}

function CrosstabRowBlock({ row, dim }: { row: CrosstabRow; dim: CrosstabDimension }) {
  return (
    <div className="rounded-lg border border-border/50 p-3">
      <div className="mb-2 flex flex-wrap items-center gap-2">
        <span className="font-medium text-[13px]">{row.errorType}</span>
        <Badge variant="secondary">{formatNumber(row.total)} 次</Badge>
        <span className="text-[11px] text-muted-foreground">
          集中度 {(row.concentration * 100).toFixed(0)}% · 覆盖 {row.distinctKeys} 个对象
        </span>
      </div>
      <div className="space-y-1">
        {row.buckets.map((b) => (
          <CrosstabBucketRow key={b.key} bucket={b} dim={dim} />
        ))}
      </div>
    </div>
  )
}

/**
 * 样本量不足的原因。分子（错误数）与分母（流量）任一过小，lift 都不可信，
 * 但两者的原因不同，提示文案也应不同 —— 否则用户看到「样本过小」会去看流量，
 * 而真正的问题可能在错误数只有 1。
 */
/**
 * lift 在该桶上是否根本没有意义（区别于「样本不足所以不可信」）。
 *
 * endpoint 维度的空串桶 = 请求没走到上游。这类请求按定义 100% 失败，
 * 错误数恒等于流量数，于是 lift 必然是个巨大的数（实测 380）—— 它是同义反复，
 * 不是信号，但因为满足「100% 失败率」豁免又不会被样本守卫拦住，
 * 会以最高 lift 出现在面板上，抢掉真实信号的注意力。
 */
function liftIsMeaningless(b: CrosstabBucket, dim: CrosstabDimension): boolean {
  return dim === 'endpoint' && b.key === ''
}

function liftSampleWarning(b: CrosstabBucket): string | null {
  // 分子守卫无豁免：错误数太少，无论分母多大都不成模式
  if (b.count < LIFT_MIN_ERRORS) {
    return `仅 ${b.count} 个错误，不足以构成模式（需 ≥${LIFT_MIN_ERRORS}）`
  }
  // 分母守卫有一个豁免：失败率 100% 时流量小不影响结论。
  // 实测代理 10101 是 10 次请求 10 次全失败（已核实为真故障），流量 10 < 30
  // 会被分母守卫误压成噪声 —— 但连续 10 次全失败不可能是巧合。
  // 对照组：direct 是 1/1 全失败，被上面的分子守卫正确挡住，不会因本豁免漏出。
  const allFailed = b.traffic > 0 && b.count >= b.traffic
  if (!allFailed && b.traffic > 0 && b.traffic < LIFT_MIN_TRAFFIC) {
    return `流量仅 ${b.traffic}，倍数由过小的分母算出（需 ≥${LIFT_MIN_TRAFFIC}）`
  }
  return null
}

function CrosstabBucketRow({
  bucket,
  dim,
}: {
  bucket: CrosstabBucket
  dim: CrosstabDimension
}) {
  const meaningless = liftIsMeaningless(bucket, dim)
  const lift = meaningless ? null : bucket.lift
  const warning = meaningless ? null : liftSampleWarning(bucket)
  // 样本不足时一律不着色：着色等于在说"这里有问题"，而噪声不该触发告警观感
  const tone =
    lift == null || warning != null
      ? 'text-muted-foreground'
      : lift >= LIFT_HIGH
        ? 'text-red-600 dark:text-red-400 font-semibold'
        : lift < LIFT_NORMAL
          ? 'text-muted-foreground'
          : 'text-amber-600 dark:text-amber-400'

  return (
    <div className="flex items-center gap-2 text-[12px]">
      <span className="min-w-0 flex-1 truncate" title={bucket.key}>
        {bucketLabel(bucket, dim)}
      </span>
      <span className="shrink-0 tabular-nums text-muted-foreground">
        错误 {formatNumber(bucket.count)} / 流量 {formatNumber(bucket.traffic)}
      </span>
      <span className={cn('w-[112px] shrink-0 text-right tabular-nums', tone)}>
        {meaningless ? (
          <span title="这类请求按定义 100% 失败，lift 无意义">lift —</span>
        ) : lift == null ? (
          'lift n/a'
        ) : (
          `lift ${lift.toFixed(2)}`
        )}
        {warning && (
          <span className="ml-1 cursor-help text-muted-foreground" title={warning}>
            ⚠
          </span>
        )}
      </span>
    </div>
  )
}

function relTime(iso: string): string {
  const d = new Date(iso).getTime()
  if (Number.isNaN(d)) return iso
  const mins = Math.floor((Date.now() - d) / 60000)
  if (mins < 1) return '刚刚'
  if (mins < 60) return `${mins} 分钟前`
  const hrs = Math.floor(mins / 60)
  if (hrs < 24) return `${hrs} 小时前`
  return `${Math.floor(hrs / 24)} 天前`
}

/**
 * 错误指纹面板。
 *
 * 面板存在的理由：error_type 只有十几种取值，看不出「一个上游错误刷了几百次」
 * 与「几百个互不相同的错误」的区别，而两者处置动作相反。
 *
 * 原始样本默认折叠但必须可展开：归一化规则可能把不同根因误并成一个指纹，
 * 样本是唯一能发现这件事的途径。
 */
function ErrorFingerprintPanel({ hours }: { hours: number }) {
  const { data } = useOpsErrorFingerprints(hours, 50)
  const rows = data?.rows ?? []
  const [expanded, setExpanded] = useState<string | null>(null)

  return (
    <Card>
      <CardContent className="p-4 sm:p-5">
        <div className="mb-3">
          <h2 className="text-base font-semibold tracking-tight">错误指纹</h2>
          <p className="text-[12px] text-muted-foreground">
            消息归一化后归并。点开可看原始消息 —— 若同一指纹下的样本根因不同，
            说明归一化过度，需要收紧规则
          </p>
        </div>
        {rows.length === 0 ? (
          <div className="py-8 text-center text-sm text-muted-foreground">窗口内无错误消息</div>
        ) : (
          <div className="space-y-2">
            {rows.map((r) => (
              <FingerprintRow
                key={r.fingerprint}
                row={r}
                open={expanded === r.fingerprint}
                onToggle={() =>
                  setExpanded(expanded === r.fingerprint ? null : r.fingerprint)
                }
              />
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  )
}

function FingerprintRow({
  row,
  open,
  onToggle,
}: {
  row: ErrorFingerprint
  open: boolean
  onToggle: () => void
}) {
  return (
    <div className="rounded-lg border border-border/50">
      <button
        type="button"
        onClick={onToggle}
        className="flex w-full items-start gap-2 p-3 text-left hover:bg-muted/40"
      >
        <Badge variant="secondary" className="mt-0.5 shrink-0 tabular-nums">
          {formatNumber(row.count)}
        </Badge>
        {row.httpStatus != null && (
          <Badge
            variant={row.httpStatus >= 500 ? 'destructive' : 'outline'}
            className="mt-0.5 shrink-0 tabular-nums"
          >
            {row.httpStatus}
          </Badge>
        )}
        <span className="min-w-0 flex-1 break-all text-[12px] leading-relaxed">
          {row.fingerprint}
        </span>
      </button>
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1 px-3 pb-2 text-[11px] text-muted-foreground">
        <span>最近 {relTime(row.lastSeen)}</span>
        <span>首次 {relTime(row.firstSeen)}</span>
        <span>
          类型 {row.errorTypes.join(' / ')}
          {row.errorTypes.length > 1 && (
            <span className="ml-1 text-amber-600" title="同一指纹映射到多个 error_type，说明分类与消息内容有歧义">
              ⚠
            </span>
          )}
        </span>
        <span className="text-muted-foreground/70">{open ? '收起' : `${row.samples.length} 条原始样本`}</span>
      </div>
      {open && (
        <div className="space-y-1.5 border-t border-border/40 px-3 py-2">
          {row.samples.map((s, i) => (
            <pre
              key={i}
              className="overflow-x-auto whitespace-pre-wrap break-all rounded bg-muted/50 p-2 text-[11px] leading-relaxed"
            >
              {s}
            </pre>
          ))}
        </div>
      )}
    </div>
  )
}

/**
 * 重试有效性阶梯。
 *
 * 面板直接给结论而不是只摆数字：运维要的是「第几跳开始白等」，
 * 自己从 reached/rescued 两列心算边际收益不现实。
 */
function RetryLadderPanel({ hours }: { hours: number }) {
  const { data } = useOpsRetryEffectiveness(hours)
  const steps = data?.steps ?? []
  const maxReached = steps[0]?.reached ?? 0

  return (
    <Card>
      <CardContent className="p-4 sm:p-5">
        <div className="mb-3">
          <h2 className="text-base font-semibold tracking-tight">重试有效性</h2>
          <p className="text-[12px] text-muted-foreground">
            边际收益 = 只有跑到这一跳才成功的比例。塌到很低说明该跳在白等，
            代价是用户多等一轮 backoff
          </p>
        </div>
        {steps.length === 0 ? (
          <div className="py-8 text-center text-sm text-muted-foreground">窗口内无请求</div>
        ) : (
          <>
            <div className="space-y-2">
              {steps.map((s) => (
                <RetryStepRow
                  key={s.attempt}
                  step={s}
                  maxReached={maxReached}
                  collapsed={data?.yieldCollapseAt != null && s.attempt >= data.yieldCollapseAt}
                />
              ))}
            </div>
            <RetrySummary data={data} />
          </>
        )}
      </CardContent>
    </Card>
  )
}

function RetryStepRow({
  step,
  maxReached,
  collapsed,
}: {
  step: RetryLadderStep
  maxReached: number
  collapsed: boolean
}) {
  // 条宽按到达数占首跳的比例，直观体现「越往后越少人走到」
  const widthPct = maxReached > 0 ? Math.max(1, (step.reached / maxReached) * 100) : 0
  const yieldPct = (step.marginalYield * 100).toFixed(1)
  return (
    <div className="flex items-center gap-3 text-[12px]">
      <span className="w-12 shrink-0 text-muted-foreground">第 {step.attempt} 跳</span>
      <div className="relative h-6 min-w-0 flex-1 overflow-hidden rounded bg-muted/40">
        <div
          className={cn(
            'h-full rounded transition-all',
            collapsed ? 'bg-red-500/25' : 'bg-emerald-500/25',
          )}
          style={{ width: `${widthPct}%` }}
        />
        <span className="absolute inset-y-0 left-2 flex items-center tabular-nums text-muted-foreground">
          到达 {formatNumber(step.reached)} · 救回 {formatNumber(step.rescued)}
        </span>
      </div>
      <span
        className={cn(
          'w-24 shrink-0 text-right tabular-nums',
          collapsed ? 'font-semibold text-red-600 dark:text-red-400' : 'text-muted-foreground',
        )}
      >
        收益 {yieldPct}%
      </span>
    </div>
  )
}

function RetrySummary({ data }: { data?: RetryEffectiveness }) {
  if (!data) return null
  const collapse = data.yieldCollapseAt
  return (
    <div className="mt-3 space-y-1.5 border-t border-border/40 pt-3 text-[12px]">
      <div className="flex flex-wrap gap-x-4 gap-y-1 text-muted-foreground">
        <span>
          重试救回 <strong className="tabular-nums text-foreground">{formatNumber(data.totalRescued)}</strong> 个请求
        </span>
        <span>
          跑满仍失败 <strong className="tabular-nums text-foreground">{formatNumber(data.totalExhausted)}</strong> 个
        </span>
      </div>
      {collapse != null && (
        <p className="rounded-md bg-amber-500/10 px-2.5 py-1.5 text-[11px] text-amber-600">
          第 <strong className="mx-0.5">{collapse}</strong> 跳起边际收益已塌。下调重试上限可省掉这些等待，
          代价是放弃该跳救回的那部分请求 —— 取舍取决于你更在意延迟还是成功率。
        </p>
      )}
      {data.backoffCoverage <= 0 ? (
        <p className="text-[11px] text-muted-foreground">
          backoff 时长无数据（覆盖率 0）：started_ms 是新增列，多跳记录尚未积累。
          攒够后此处会显示各跳的等待中位数。
        </p>
      ) : (
        data.backoffCoverage < 0.8 && (
          <p className="text-[11px] text-amber-600">
            backoff 覆盖率仅 {(data.backoffCoverage * 100).toFixed(0)}%，时长数据不完整
          </p>
        )
      )}
    </div>
  )
}

/** 按凭据统计表 */
function CredentialTable({ rows }: { rows: OpsCredentialRow[] }) {
  return (
    <Card>
      <CardContent className="p-4">
        <div className="mb-3 text-sm font-medium">按凭据（上游问题归属）</div>
        {rows.length === 0 ? (
          <div className="py-6 text-center text-sm text-muted-foreground">窗口内无数据</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-[13px]">
              <thead>
                <tr className="border-b border-border/60 text-left text-xs text-muted-foreground">
                  <th className="py-1.5 pr-2 font-normal">凭据</th>
                  <th className="py-1.5 pr-2 text-right font-normal">请求</th>
                  <th className="py-1.5 pr-2 text-right font-normal">成功率</th>
                  <th className="py-1.5 pr-2 text-right font-normal">中断/截断</th>
                  <th className="py-1.5 pr-2 text-right font-normal">鉴权</th>
                  <th className="py-1.5 pr-2 text-right font-normal">风控</th>
                  <th className="py-1.5 pr-2 text-right font-normal">网络</th>
                  <th className="py-1.5 text-right font-normal">其他</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.credentialId} className="border-b border-border/30 last:border-0">
                    <td className="max-w-[220px] truncate py-1.5 pr-2">
                      <span className="font-mono text-muted-foreground">#{r.credentialId}</span>{' '}
                      {r.email ?? ''}
                    </td>
                    <td className="py-1.5 pr-2 text-right font-mono">{formatNumber(r.total)}</td>
                    <td className="py-1.5 pr-2 text-right font-mono">{pct(r.success, r.total)}</td>
                    <td
                      className={cn(
                        'py-1.5 pr-2 text-right font-mono',
                        r.interrupted > 0 && 'text-amber-500',
                      )}
                    >
                      {formatNumber(r.interrupted)}
                    </td>
                    <td className="py-1.5 pr-2 text-right font-mono">{formatNumber(r.authFailed)}</td>
                    <td className="py-1.5 pr-2 text-right font-mono">
                      {formatNumber(r.accountThrottled)}
                    </td>
                    <td className="py-1.5 pr-2 text-right font-mono">
                      {formatNumber(r.networkError)}
                    </td>
                    <td className="py-1.5 text-right font-mono">{formatNumber(r.otherFailed)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </CardContent>
    </Card>
  )
}

/** 按代理统计表（流量统计 + 池内实时状态合并） */
function ProxyTable({
  stats,
  pool,
}: {
  stats: OpsProxyRow[]
  pool: ProxyPoolEntry[]
}) {
  const poolByUrl = useMemo(() => {
    const m = new Map<string, ProxyPoolEntry>()
    for (const p of pool) m.set(p.url, p)
    return m
  }, [pool])

  // 有流量的在前；池里有但窗口内无流量的也列出（补零行）
  const rows = useMemo(() => {
    const seen = new Set(stats.map((s) => s.proxyUrl))
    const extra: OpsProxyRow[] = pool
      .filter((p) => !seen.has(p.url))
      .map((p) => ({
        proxyUrl: p.url,
        attempts: 0,
        success: 0,
        networkError: 0,
        otherFailed: 0,
        interrupted: 0,
      }))
    return [...stats, ...extra]
  }, [stats, pool])

  return (
    <Card>
      <CardContent className="p-4">
        <div className="mb-3 text-sm font-medium">按代理（链路问题归属）</div>
        {rows.length === 0 ? (
          <div className="py-6 text-center text-sm text-muted-foreground">窗口内无数据</div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-[13px]">
              <thead>
                <tr className="border-b border-border/60 text-left text-xs text-muted-foreground">
                  <th className="py-1.5 pr-2 font-normal">代理</th>
                  <th className="py-1.5 pr-2 font-normal">状态</th>
                  <th className="py-1.5 pr-2 text-right font-normal">请求跳</th>
                  <th className="py-1.5 pr-2 text-right font-normal">网络错误</th>
                  <th className="py-1.5 pr-2 text-right font-normal">中断/截断</th>
                  <th className="py-1.5 pr-2 text-right font-normal">连败(请求)</th>
                  <th className="py-1.5 text-right font-normal">绑定凭据</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => {
                  const p = r.proxyUrl ? poolByUrl.get(r.proxyUrl) : undefined
                  return (
                    <tr key={r.proxyUrl || '__direct'} className="border-b border-border/30 last:border-0">
                      <td className="max-w-[260px] truncate py-1.5 pr-2 font-mono">
                        {r.proxyUrl || <span className="text-muted-foreground">直连</span>}
                      </td>
                      <td className="py-1.5 pr-2">
                        {r.proxyUrl === '' ? (
                          <span className="text-muted-foreground">—</span>
                        ) : p ? (
                          <span className="flex flex-wrap items-center gap-1">
                            <Badge
                              variant={
                                !p.enabled
                                  ? 'destructive'
                                  : p.health === 'healthy'
                                    ? 'success'
                                    : p.health === 'unhealthy'
                                      ? 'warning'
                                      : 'outline'
                              }
                            >
                              {!p.enabled
                                ? p.autoDisabled
                                  ? '自动禁用'
                                  : '已禁用'
                                : p.health === 'healthy'
                                  ? '健康'
                                  : p.health === 'unhealthy'
                                    ? '异常'
                                    : '未探测'}
                            </Badge>
                            {p.latencyMs != null && p.enabled ? (
                              <span className="text-[11px] text-muted-foreground">{p.latencyMs}ms</span>
                            ) : null}
                          </span>
                        ) : (
                          <Badge variant="outline">不在池内</Badge>
                        )}
                      </td>
                      <td className="py-1.5 pr-2 text-right font-mono">{formatNumber(r.attempts)}</td>
                      <td
                        className={cn(
                          'py-1.5 pr-2 text-right font-mono',
                          r.networkError > 0 && 'text-destructive',
                        )}
                      >
                        {formatNumber(r.networkError)}
                      </td>
                      <td
                        className={cn(
                          'py-1.5 pr-2 text-right font-mono',
                          r.interrupted > 0 && 'text-amber-500',
                        )}
                      >
                        {formatNumber(r.interrupted)}
                      </td>
                      <td className="py-1.5 pr-2 text-right font-mono">
                        {p ? p.requestFailures : '—'}
                      </td>
                      <td className="py-1.5 text-right font-mono">{p ? p.credentialCount : '—'}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </CardContent>
    </Card>
  )
}

/** 最近上游错误：错误内容明细（复用 traces API，逐跳展示错误体与出口代理） */
function RecentErrorList() {
  const [expanded, setExpanded] = useState<Set<string>>(new Set())
  // traces API 无时间窗参数，按最近 N 条失败取；与上方窗口统计互补（那边看量，这边看内容）
  const { data } = useTraces({ onlyFailed: true, limit: 30 })
  const records = data?.records ?? []

  const toggle = (id: string) =>
    setExpanded((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })

  return (
    <Card>
      <CardContent className="p-4">
        <div className="mb-3 flex items-center gap-2">
          <span className="text-sm font-medium">最近上游错误</span>
          <span className="text-[11px] text-muted-foreground">
            最近 {records.length} 条失败请求（点击展开每跳错误体与出口）
          </span>
        </div>
        {records.length === 0 ? (
          <div className="py-6 text-center text-sm text-muted-foreground">暂无失败请求</div>
        ) : (
          <div className="space-y-1.5">
            {records.map((r: TraceRecord) => {
              const open = expanded.has(r.traceId)
              return (
                <div
                  key={r.traceId}
                  className="rounded-lg border border-border/50 bg-secondary/30 text-[13px]"
                >
                  <button
                    type="button"
                    onClick={() => toggle(r.traceId)}
                    className="flex w-full flex-wrap items-center gap-2 p-2.5 text-left hover:bg-secondary/50"
                  >
                    <Badge
                      variant={
                        r.errorType === 'stream_interrupted' ||
                        r.errorType === 'upstream_truncated'
                          ? 'warning'
                          : 'destructive'
                      }
                    >
                      {ERROR_TYPE_LABELS[r.errorType ?? ''] ?? r.errorType ?? r.finalStatus}
                    </Badge>
                    <span className="font-mono text-[12px] text-muted-foreground">{r.model}</span>
                    {r.finalCredentialId > 0 && (
                      <span className="text-[12px] text-muted-foreground">
                        凭据 #{r.finalCredentialId}
                        {r.finalEmail ? ` ${r.finalEmail}` : ''}
                      </span>
                    )}
                    <span className="ml-auto shrink-0 font-mono text-[11px] text-muted-foreground">
                      {formatDuration(r.durationMs)} · {new Date(r.ts).toLocaleString('zh-CN')}
                    </span>
                  </button>
                  <div className="break-all px-2.5 pb-2.5 text-muted-foreground">
                    {r.errorMessage ?? '（无错误信息）'}
                  </div>
                  {open && r.attempts.length > 0 && (
                    <div className="space-y-1.5 border-t border-border/40 p-2.5">
                      {r.attempts.map((a) => (
                        <div key={a.attempt} className="rounded-md bg-background/60 p-2">
                          <div className="flex flex-wrap items-center gap-2 text-[12px]">
                            <span className="font-mono text-muted-foreground">#{a.attempt}</span>
                            <Badge variant="outline">{a.outcome}</Badge>
                            <span className="text-muted-foreground">
                              凭据 #{a.credentialId} · HTTP {a.httpStatus ?? '—'}
                            </span>
                            <span className="text-muted-foreground">出口</span>
                            <span
                              className="max-w-[200px] truncate font-mono"
                              title={a.proxyUrl ?? '直连'}
                            >
                              {a.proxyUrl ?? '直连'}
                            </span>
                          </div>
                          {a.errorSnippet && (
                            <pre className="mt-1.5 max-h-40 overflow-auto whitespace-pre-wrap break-all font-mono text-[11px] text-muted-foreground">
                              {a.errorSnippet}
                            </pre>
                          )}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )
            })}
          </div>
        )}
      </CardContent>
    </Card>
  )
}

/** 自动处置事件列表 */
function EventList({ events }: { events: OpsEvent[] }) {
  return (
    <Card>
      <CardContent className="p-4">
        <div className="mb-3 text-sm font-medium">自动处置事件</div>
        {events.length === 0 ? (
          <div className="py-6 text-center text-sm text-muted-foreground">
            暂无处置事件（代理自动禁用 / 凭据换绑会记录在这里）
          </div>
        ) : (
          <div className="space-y-2">
            {events.map((e) => (
              <div
                key={e.id}
                className="rounded-lg border border-border/50 bg-secondary/30 p-2.5 text-[13px]"
              >
                <div className="flex flex-wrap items-center gap-2">
                  <Badge variant={e.severity === 'error' ? 'destructive' : 'warning'}>
                    {EVENT_CATEGORY_LABELS[e.category] ?? e.category}
                  </Badge>
                  <span className="font-mono text-muted-foreground">{e.subject}</span>
                  <span className="ml-auto text-[11px] text-muted-foreground">
                    {new Date(e.ts).toLocaleString('zh-CN')}
                  </span>
                </div>
                <div className="mt-1 break-all text-muted-foreground">{e.message}</div>
              </div>
            ))}
          </div>
        )}
      </CardContent>
    </Card>
  )
}

/** 运维页：上游问题 + 代理池问题的统一统计与处置留痕 */
export function OpsPage() {
  const [hours, setHours] = useState(24)
  const { data: overview } = useOpsOverview(hours)
  const { data: credentials } = useOpsCredentials(hours)
  const { data: proxies } = useOpsProxies(hours)
  const { data: events } = useOpsEvents(100)

  const total = overview?.total ?? 0
  const failed = (overview?.error ?? 0) + (overview?.interrupted ?? 0)

  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-2">
        <div className="text-sm font-medium">统计窗口</div>
        <div className="flex items-center gap-1 rounded-full border border-border/60 p-0.5">
          {WINDOWS.map((w) => (
            <Button
              key={w.hours}
              size="sm"
              variant={hours === w.hours ? 'default' : 'ghost'}
              className="h-7 rounded-full px-3 text-xs"
              onClick={() => setHours(w.hours)}
            >
              {w.label}
            </Button>
          ))}
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6">
        <StatCard icon={<Activity className="h-4 w-4" />} label="总请求" value={formatNumber(total)} />
        <StatCard
          icon={<ShieldAlert className="h-4 w-4" />}
          label="失败率"
          value={pct(failed, total)}
          sub={`错误 ${formatNumber(overview?.error ?? 0)} / 中断 ${formatNumber(overview?.interrupted ?? 0)}`}
          tone={failed > 0 ? 'warn' : 'ok'}
        />
        <StatCard
          icon={<ScissorsLineDashed className="h-4 w-4" />}
          label="流中断/截断"
          value={formatNumber(
            (overview?.byErrorType ?? [])
              .filter((e) => e.errorType === 'stream_interrupted' || e.errorType === 'upstream_truncated')
              .reduce((s, e) => s + e.count, 0),
          )}
          tone="warn"
        />
        <StatCard
          icon={<Timer className="h-4 w-4" />}
          label="中断耗时 p95"
          value={formatDuration(overview?.interruptedDuration?.p95)}
          sub={interruptedDurationSub(overview?.interruptedDuration)}
          tone={overview?.interruptedDuration ? 'bad' : undefined}
        />
        <StatCard
          icon={<Network className="h-4 w-4" />}
          label="平均耗时"
          value={formatDuration(overview?.avgDurationMs)}
        />
        <StatCard
          icon={<AlertTriangle className="h-4 w-4" />}
          label="平均首Token"
          value={formatDuration(overview?.avgFirstTokenMs)}
        />
      </div>

      <div className="grid gap-4 xl:grid-cols-3">
        <div className="xl:col-span-2">
          <TrendChart hours={hours} />
        </div>
        <ErrorTypeList hours={hours} />
      </div>

      <ErrorCrosstabPanel hours={hours} />
      <ErrorFingerprintPanel hours={hours} />
      <RetryLadderPanel hours={hours} />
      <CredentialTable rows={credentials ?? []} />
      <ProxyTable stats={proxies?.stats ?? []} pool={proxies?.pool.proxies ?? []} />
      <RecentErrorList />
      <EventList events={events ?? []} />
    </div>
  )
}
