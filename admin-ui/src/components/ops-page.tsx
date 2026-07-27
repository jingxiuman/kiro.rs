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
  useOpsEvents,
  useOpsOverview,
  useOpsProxies,
  useOpsTrend,
} from '@/hooks/use-ops'
import { useTraces } from '@/hooks/use-traces'
import type {
  OpsCredentialRow,
  OpsEvent,
  OpsProxyRow,
  ProxyPoolEntry,
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

/** 按小时趋势（成功 / 错误 / 中断堆叠柱） */
function TrendChart({ hours }: { hours: number }) {
  const { data } = useOpsTrend(hours)
  const chartData = useMemo(
    () =>
      (data ?? []).map((p) => ({
        time: new Date(p.bucketEpoch * 1000).toLocaleString('zh-CN', {
          month: 'numeric',
          day: 'numeric',
          hour: 'numeric',
        }),
        成功: p.success,
        错误: p.error,
        中断: p.interrupted,
      })),
    [data],
  )
  return (
    <Card>
      <CardContent className="p-4">
        <div className="mb-3 text-sm font-medium">请求趋势（按小时）</div>
        <div className="h-56">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={chartData} margin={{ top: 4, right: 8, left: -16, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" strokeOpacity={0.25} />
              <XAxis dataKey="time" tick={{ fontSize: 11 }} />
              <YAxis tick={{ fontSize: 11 }} allowDecimals={false} />
              <Tooltip
                contentStyle={{
                  background: 'hsl(var(--card))',
                  border: '1px solid hsl(var(--border))',
                  borderRadius: 8,
                  fontSize: 12,
                }}
              />
              <Legend wrapperStyle={{ fontSize: 12 }} />
              <Bar dataKey="成功" stackId="a" fill="#22c55e" />
              <Bar dataKey="错误" stackId="a" fill="#ef4444" />
              <Bar dataKey="中断" stackId="a" fill="#f59e0b" />
            </BarChart>
          </ResponsiveContainer>
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
          label="中断平均时长"
          value={formatDuration(overview?.interruptedAvgDurationMs)}
          sub="集中在同一时长 → 链路固定超时"
          tone={overview?.interruptedAvgDurationMs ? 'bad' : undefined}
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

      <CredentialTable rows={credentials ?? []} />
      <ProxyTable stats={proxies?.stats ?? []} pool={proxies?.pool.proxies ?? []} />
      <RecentErrorList />
      <EventList events={events ?? []} />
    </div>
  )
}
