import { useState } from 'react'
import axios from 'axios'
import { toast } from 'sonner'
import {
  ScrollText,
  RefreshCw,
  ChevronRight,
  ChevronLeft,
  ChevronDown,
  AlertTriangle,
  CheckCircle2,
  FileJson,
  Unplug,
  Settings2,
} from 'lucide-react'
import { Card, CardContent } from '@/components/ui/card'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Input } from '@/components/ui/input'
import { Switch } from '@/components/ui/switch'
import {
  DropdownMenu,
  DropdownMenuTrigger,
  DropdownMenuContent,
  DropdownMenuLabel,
} from '@/components/ui/dropdown-menu'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import {
  Select as UiSelect,
  SelectTrigger as UiSelectTrigger,
  SelectValue as UiSelectValue,
  SelectContent as UiSelectContent,
  SelectItem as UiSelectItem,
} from '@/components/ui/select'
import { useTraces } from '@/hooks/use-traces'
import { getTraceRequestBody } from '@/api/traces'
import { useClientKeys } from '@/hooks/use-client-keys'
import { useGroupOptions } from '@/hooks/use-groups'
import {
  useLogGovernanceConfig,
  useSetLogGovernanceConfig,
} from '@/hooks/use-credentials'
import { extractErrorMessage } from '@/lib/utils'
import { formatDuration } from '@/lib/format'
import { TracePhaseLane } from '@/components/trace-phase-lane'
import { TraceTimingBar } from '@/components/trace-timing-bar'
import { usePhaseBaseline } from '@/hooks/use-ops'
import type {
  PhaseBaselineRow,
  StreamShapeBlock,
  TraceAttempt,
  TraceQuery,
  TraceRecord,
} from '@/types/api'

/** 失败分类 → 中文标签 + Badge 颜色 */
function outcomeStyle(outcome: string): {
  label: string
  variant: 'default' | 'secondary' | 'destructive' | 'outline' | 'success' | 'warning'
} {
  switch (outcome) {
    case 'success':
      return { label: '成功', variant: 'success' }
    case 'quota_exhausted':
      return { label: '额度耗尽', variant: 'warning' }
    case 'account_throttled':
      return { label: '账号风控', variant: 'warning' }
    case 'rate_limited':
      return { label: '上游限流', variant: 'warning' }
    case 'auth_failed':
      return { label: '鉴权失败', variant: 'destructive' }
    case 'transient':
      return { label: '瞬态错误', variant: 'outline' }
    case 'network_error':
      return { label: '网络错误', variant: 'destructive' }
    case 'bad_request':
      return { label: '请求错误', variant: 'destructive' }
    case 'stream_interrupted':
      return { label: '流中断', variant: 'warning' }
    case 'upstream_truncated':
      return { label: '上游截断', variant: 'warning' }
    case 'upstream_invalid':
      return { label: '上游非法JSON', variant: 'destructive' }
    default:
      return { label: outcome || '未知', variant: 'secondary' }
  }
}

/** 最终状态 → 徽章 */
function StatusBadge({ status }: { status: string }) {
  if (status === 'success')
    return (
      <Badge variant="success">
        <CheckCircle2 className="mr-1 h-3 w-3" />
        成功
      </Badge>
    )
  if (status === 'interrupted')
    return (
      <Badge variant="warning">
        <Unplug className="mr-1 h-3 w-3" />
        中断
      </Badge>
    )
  return (
    <Badge variant="destructive">
      <AlertTriangle className="mr-1 h-3 w-3" />
      失败
    </Badge>
  )
}

function formatTime(ts: string): string {
  const d = new Date(ts)
  if (isNaN(d.getTime())) return ts
  return d.toLocaleString('zh-CN', { hour12: false })
}

function formatTokens(n: number): string {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(2) + 'M'
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K'
  return String(n)
}

/** 千位分隔的完整数值（用于明细悬浮框） */
function formatTokenFull(n: number): string {
  return n.toLocaleString('en-US')
}

function credLabel(id: number, email?: string | null): string {
  if (id === 0) return '—'
  return email ? email : `#${id}`
}

function keyLabel(
  keyId: number,
  keyName?: string | null,
  keySource?: TraceRecord['keySource'],
): string {
  if (keySource === 'internal') return '系统任务'
  if (keyName) return keyName
  return `#${keyId}`
}

function requestLabel(rec: TraceRecord): { label: string; detail?: string } {
  switch (rec.operation) {
    case 'balance_refresh':
      return { label: '余额刷新', detail: 'getUsageLimits' }
    case 'token_refresh':
      return { label: 'Token 刷新', detail: rec.attempts[0]?.endpoint }
    default:
      return { label: rec.model || '推理请求' }
  }
}

const STATUS_OPTIONS = [
  { value: '', label: '全部状态' },
  { value: 'success', label: '成功' },
  { value: 'error', label: '失败' },
  { value: 'interrupted', label: '中断' },
]

const OPERATION_OPTIONS = [
  { value: '', label: '全部请求类型' },
  { value: 'inference', label: '推理请求' },
  { value: 'balance_refresh', label: '余额刷新' },
  { value: 'token_refresh', label: 'Token 刷新' },
]

const ERROR_TYPE_OPTIONS = [
  { value: '', label: '全部错误类型' },
  { value: 'quota_exhausted', label: '额度耗尽' },
  { value: 'account_throttled', label: '账号风控' },
  { value: 'rate_limited', label: '上游限流' },
  { value: 'auth_failed', label: '鉴权失败' },
  { value: 'transient', label: '瞬态错误' },
  { value: 'network_error', label: '网络错误' },
  { value: 'bad_request', label: '请求错误' },
  { value: 'stream_interrupted', label: '流中断' },
  { value: 'upstream_truncated', label: '上游截断' },
  { value: 'upstream_invalid', label: '上游非法JSON' },
  { value: 'unknown', label: '未知' },
]

/** 出口三态：direct = 直连；null/undefined = 未知（该列存在前的历史行）；其余 = 代理 URL */
function ProxyLabel({ url }: { url?: string | null }) {
  if (url == null) {
    return (
      <span className="font-mono text-[12px] text-muted-foreground/60" title="该记录早于出口埋点，真实出口不可知">
        未知
      </span>
    )
  }
  const text = url === 'direct' ? '直连' : url
  return (
    <span className="max-w-[220px] truncate font-mono text-[12px]" title={text}>
      {text}
    </span>
  )
}

/** 单跳明细行 */
function AttemptRow({ a }: { a: TraceAttempt }) {
  const style = outcomeStyle(a.outcome)
  return (
    <div className="rounded-lg border border-border/50 bg-secondary/30 p-3">
      <div className="flex flex-wrap items-center gap-2 text-[13px]">
        <span className="font-mono text-muted-foreground">#{a.attempt}</span>
        <Badge variant={style.variant}>{style.label}</Badge>
        <span className="text-muted-foreground">凭据</span>
        <span className="font-medium">{credLabel(a.credentialId, a.email)}</span>
        {a.endpoint && <Badge variant="outline">{a.endpoint}</Badge>}
        <span className="text-muted-foreground">HTTP</span>
        <span className="font-mono">{a.httpStatus ?? '—'}</span>
        <span className="text-muted-foreground">出口</span>
        <ProxyLabel url={a.proxyUrl} />
        <span className="ml-auto font-mono text-muted-foreground">
          {formatDuration(a.durationMs)}
        </span>
      </div>
      {a.errorSnippet && (
        <pre className="mt-2 max-h-40 overflow-auto whitespace-pre-wrap break-all rounded-md bg-background/60 p-2 font-mono text-[11px] text-muted-foreground">
          {a.errorSnippet}
        </pre>
      )}
    </div>
  )
}

/** 可展开的链路行 */
/** Token 用量单元格：紧凑展示总量，hover 显示分项明细 */
function TokenCell({ rec }: { rec: TraceRecord }) {
  const input = rec.inputTokens ?? 0
  const output = rec.outputTokens ?? 0
  const cacheCreation = rec.cacheCreationTokens ?? 0
  const cacheRead = rec.cacheReadTokens ?? 0
  const total = rec.totalTokens ?? input + output + cacheCreation + cacheRead
  // 全 0（早期失败、未走到上游）时不显示明细，仅占位
  if (total === 0) {
    return <span className="text-muted-foreground">—</span>
  }
  const rows: Array<[string, number]> = [
    ['输入 Token', input],
    ['输出 Token', output],
  ]
  if (cacheCreation > 0) rows.push(['缓存创建 Token', cacheCreation])
  if (cacheRead > 0) rows.push(['缓存读取 Token', cacheRead])
  return (
    <TooltipProvider delayDuration={150}>
      <Tooltip>
        <TooltipTrigger asChild>
          <span className="inline-flex items-center gap-1 font-mono tabular-nums cursor-default border-b border-dotted border-muted-foreground/40">
            <span className="text-emerald-600 dark:text-emerald-400">
              ↓{formatTokens(input + cacheCreation + cacheRead)}
            </span>
            <span className="text-violet-600 dark:text-violet-400">
              ↑{formatTokens(output)}
            </span>
          </span>
        </TooltipTrigger>
        <TooltipContent className="p-0">
          <div className="min-w-[180px] px-3 py-2">
            <div className="mb-1.5 text-[13px] font-semibold">Token 明细</div>
            <div className="space-y-1 text-[12px]">
              {rows.map(([label, val]) => (
                <div key={label} className="flex items-center justify-between gap-6">
                  <span className="text-muted-foreground">{label}</span>
                  <span className="font-mono tabular-nums">{formatTokenFull(val)}</span>
                </div>
              ))}
              <div className="mt-1 flex items-center justify-between gap-6 border-t border-border/50 pt-1">
                <span className="font-medium">总 Token</span>
                <span className="font-mono font-semibold tabular-nums">
                  {formatTokenFull(total)}
                </span>
              </div>
            </div>
          </div>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  )
}

/** 假活流阈值：首个可渲染帧比首个上游 chunk 晚超过 30s，视为「流在推进但客户端无可渲染内容」 */
const FAKE_LIVE_GAP_MS = 30_000

/** 首Token / 首渲染帧单元格：两者差值过大时用告警配色标记假活流 */
function FirstTokenCell({ rec }: { rec: TraceRecord }) {
  const first = rec.firstTokenMs
  const render = rec.firstRenderMs
  const fakeLive = first != null && render != null && render - first > FAKE_LIVE_GAP_MS
  return (
    <td className="py-2.5 pr-3 text-[13px] tabular-nums text-muted-foreground">
      {first != null ? formatDuration(first) : '—'}
      {render != null && (
        <span
          className={
            fakeLive
              ? 'ml-1 font-medium text-amber-600 dark:text-amber-400'
              : 'ml-1 text-muted-foreground/70'
          }
          title={
            fakeLive && first != null
              ? `首个可渲染帧比首个上游 chunk 晚 ${formatDuration(render - first)}，疑似假活流`
              : `首个可渲染帧 ${formatDuration(render)}`
          }
        >
          /{formatDuration(render)}
        </span>
      )}
    </td>
  )
}

/** 流形态块类型 → 配色（与 TokenCell 的输入/输出色系保持同族） */
const SHAPE_TYPE_STYLE: Record<string, string> = {
  thinking: 'text-violet-600 dark:text-violet-400',
  text: 'text-emerald-600 dark:text-emerald-400',
  tool_use: 'text-sky-600 dark:text-sky-400',
  redacted_thinking: 'text-muted-foreground',
}

/** 流形态紧凑展示：thinking@4.6s(308B) → text@5.7s(299B) */
function StreamShapeLine({ shape }: { shape: StreamShapeBlock[] }) {
  return (
    <div className="flex flex-wrap items-center gap-x-1.5 gap-y-1 font-mono text-[12px]">
      {shape.map((blk, i) => (
        <span key={i} className="inline-flex items-center gap-1.5">
          {i > 0 && <span className="text-muted-foreground/50">→</span>}
          <span className={SHAPE_TYPE_STYLE[blk.t] ?? 'text-foreground/80'}>
            {blk.t}@{(blk.ms / 1000).toFixed(1)}s({blk.b}B)
          </span>
        </span>
      ))}
    </div>
  )
}

/** 查看原始请求体：懒加载调 request-body 端点，404 = 未启用保留或已过期 */
function RequestBodyButton({ traceId }: { traceId: string }) {
  const [open, setOpen] = useState(false)
  const [loading, setLoading] = useState(false)
  const [body, setBody] = useState('')

  const view = async () => {
    setLoading(true)
    try {
      const data = await getTraceRequestBody(traceId)
      setBody(typeof data === 'string' ? data : JSON.stringify(data, null, 2))
      setOpen(true)
    } catch (err) {
      if (axios.isAxiosError(err) && err.response?.status === 404) {
        toast.error('请求体不可用：未启用保留（storeRequestBodies）或已过期')
      } else {
        toast.error('读取请求体失败：' + extractErrorMessage(err))
      }
    } finally {
      setLoading(false)
    }
  }

  return (
    <>
      <Button
        size="sm"
        variant="outline"
        className="h-7 text-xs"
        disabled={loading}
        onClick={(e) => {
          e.stopPropagation()
          view()
        }}
      >
        <FileJson className="h-3.5 w-3.5" />
        {loading ? '读取中…' : '查看请求体'}
      </Button>
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent className="max-w-3xl">
          <DialogHeader>
            <DialogTitle>原始请求体</DialogTitle>
            <DialogDescription className="font-mono text-[12px]">{traceId}</DialogDescription>
          </DialogHeader>
          <pre className="max-h-[60vh] overflow-auto whitespace-pre-wrap break-all rounded-lg bg-secondary/40 p-3 font-mono text-[11px]">
            {body}
          </pre>
        </DialogContent>
      </Dialog>
    </>
  )
}

function TraceRow({ rec, baseline }: { rec: TraceRecord; baseline: PhaseBaselineRow[] | undefined }) {
  const [open, setOpen] = useState(false)
  const errStyle = rec.errorType ? outcomeStyle(rec.errorType) : null
  const request = requestLabel(rec)
  return (
    <>
      <tr
        className="cursor-pointer whitespace-nowrap border-b border-border/40 hover:bg-accent/40"
        onClick={() => setOpen((v) => !v)}
      >
        <td className="py-2.5 pl-3 pr-2">
          {open ? (
            <ChevronDown className="h-4 w-4 text-muted-foreground" />
          ) : (
            <ChevronRight className="h-4 w-4 text-muted-foreground" />
          )}
        </td>
        <td className="py-2.5 pr-3 text-[13px] tabular-nums text-muted-foreground whitespace-nowrap">
          {formatTime(rec.ts)}
        </td>
        <td className="py-2.5 pr-3 text-[13px]">
          <span className="inline-block max-w-[220px] truncate align-middle">{request.label}</span>
          {request.detail && (
            <span className="ml-1.5 font-mono text-[11px] text-muted-foreground">
              {request.detail}
            </span>
          )}
          {rec.operation === 'inference' && rec.isStream && <Badge variant="outline" className="ml-1.5">流式</Badge>}
        </td>
        <td className="py-2.5 pr-3 text-[13px]">
          <Badge variant="outline">{keyLabel(rec.keyId, rec.keyName, rec.keySource)}</Badge>
        </td>
        <td className="py-2.5 pr-3">
          <StatusBadge status={rec.finalStatus} />
        </td>
        <TraceCredentialCell rec={rec} />
        <td className="py-2.5 pr-3 text-[12px] tabular-nums">
          <TokenCell rec={rec} />
        </td>
        <td className="py-2.5 pr-3 text-[13px] tabular-nums">
          {rec.credits != null && rec.credits > 0 ? rec.credits.toFixed(4) : '—'}
        </td>
        <FirstTokenCell rec={rec} />
        <td className="py-2.5 pr-3">
          {errStyle ? <Badge variant={errStyle.variant}>{errStyle.label}</Badge> : '—'}
        </td>
        <td className="py-2.5 pr-3 text-[13px] tabular-nums">
          {Math.max(0, rec.totalAttempts - 1)}
        </td>
        <td className="py-2.5 pr-3 text-[13px] tabular-nums text-muted-foreground">
          {formatDuration(rec.durationMs)}
        </td>
      </tr>
      {open && <ExpandedTraceRow rec={rec} baseline={baseline} />}
    </>
  )
}

function TraceCredentialCell({ rec }: { rec: TraceRecord }) {
  return (
    <td className="py-2.5 pr-3 text-[13px]">
      <span className="inline-block max-w-[220px] truncate align-middle">
        {credLabel(rec.finalCredentialId, rec.finalEmail)}
      </span>
    </td>
  )
}

function ExpandedTraceRow({
  rec,
  baseline,
}: {
  rec: TraceRecord
  baseline: PhaseBaselineRow[] | undefined
}) {
  return (
    <tr className="border-b border-border/40 bg-secondary/20">
      <td colSpan={12} className="px-3 py-3">
        <ExpandedDetail rec={rec} baseline={baseline} />
      </td>
    </tr>
  )
}

/** 展开后的链路详情：错误摘要 + 耗时分布条 + 每跳时间线 + 分段明细 */
function ExpandedDetail({
  rec,
  baseline,
}: {
  rec: TraceRecord
  baseline: PhaseBaselineRow[] | undefined
}) {
  return (
    <div className="space-y-3">
      {rec.sessionId && (
        <div className="flex items-center gap-2 text-[12px] text-muted-foreground">
          <span>会话</span>
          <span
            className="cursor-pointer font-mono text-foreground/80"
            title={`完整会话 id: ${rec.sessionId}（点击复制）`}
            onClick={() => {
              navigator.clipboard?.writeText(rec.sessionId ?? '')
              toast.success('已复制会话 id')
            }}
          >
            {rec.sessionId.slice(0, 8)}
          </span>
          <span className="text-muted-foreground/60">
            —— compact 前后对比此 id 是否变化，可判断是否换了会话
          </span>
        </div>
      )}
      {rec.errorMessage && (
        <div className="rounded-lg border border-destructive/30 bg-destructive/5 p-3 text-[13px] text-destructive">
          {rec.errorMessage}
        </div>
      )}
      {rec.interruptedAfterBytes != null && (
        // 同一字段两条路径语义不同：流式是「已下发给客户端」，非流式一个字节都没
        // 下发过，记的是「从上游收到多少就断了」。文案必须跟着分，否则非流式会
        // 被读成「已经发了一半给客户端」——归因方向正好相反。
        <div className="text-[12px] text-muted-foreground">
          {rec.isStream
            ? `中断前已下发 ${rec.interruptedAfterBytes} 字节`
            : `中断前已从上游收到 ${rec.interruptedAfterBytes} 字节`}
        </div>
      )}
      {rec.operation === 'inference' && (
        <div className="flex items-center">
          <RequestBodyButton traceId={rec.traceId} />
        </div>
      )}
      <div className="text-[12px] font-medium text-muted-foreground">耗时分布</div>
      <TraceTimingBar rec={rec} />
      {rec.streamShape && rec.streamShape.length > 0 && (
        <>
          <div className="text-[12px] font-medium text-muted-foreground">
            流形态（{rec.streamShape.length} 块
            {rec.firstRenderMs != null ? `，首个可渲染帧 ${formatDuration(rec.firstRenderMs)}` : ''}）
          </div>
          <StreamShapeLine shape={rec.streamShape} />
        </>
      )}
      <div className="text-[12px] font-medium text-muted-foreground">
        尝试链路（{rec.attempts.length} 次
        {rec.attempts.length > 1 ? `，含 ${rec.attempts.length - 1} 次重试` : "，未重试"}）
      </div>
      <div className="space-y-2">
        {rec.attempts.length === 0 ? (
          <div className="text-[13px] text-muted-foreground">无尝试记录（请求未到达上游）</div>
        ) : (
          rec.attempts.map((a) => <AttemptRow key={a.attempt} a={a} />)
        )}
      </div>
      <div className="mt-3 text-[13px] font-medium text-muted-foreground">分段明细</div>
      <div className="mt-2">
        <TracePhaseLane
          phases={rec.phases ?? []}
          proxyUrl={rec.attempts[rec.attempts.length - 1]?.proxyUrl}
          baseline={baseline}
        />
      </div>
    </div>
  )
}

/** 下拉筛选器 */
function Select({
  value,
  onChange,
  options,
}: {
  value: string
  onChange: (v: string) => void
  options: { value: string; label: string }[]
}) {
  // radix Select 不允许空字符串 value，用哨兵 "__all__" 代表「空/全部」，对外透明。
  const SENTINEL = '__all__'
  return (
    <UiSelect
      value={value === '' ? SENTINEL : value}
      onValueChange={(v) => onChange(v === SENTINEL ? '' : v)}
    >
      <UiSelectTrigger className="h-8 w-auto min-w-[120px]">
        <UiSelectValue />
      </UiSelectTrigger>
      <UiSelectContent>
        {options.map((o) => (
          <UiSelectItem key={o.value} value={o.value === '' ? SENTINEL : o.value}>
            {o.label}
          </UiSelectItem>
        ))}
      </UiSelectContent>
    </UiSelect>
  )
}

/** 日志治理设置下拉：trace 启用开关 + trace 保留天数 + usage 保留天数 */
function GovernanceButton() {
  const [open, setOpen] = useState(false)
  const { data: cfg, isLoading } = useLogGovernanceConfig()
  const { mutate, isPending } = useSetLogGovernanceConfig()
  const [traceDays, setTraceDays] = useState('')
  const [usageDays, setUsageDays] = useState('')

  const enabled = cfg?.traceEnabled ?? true

  const save = (patch: Record<string, unknown>, ok: string) => {
    mutate(patch, {
      onSuccess: () => toast.success(ok),
      onError: (err) => toast.error('保存失败：' + extractErrorMessage(err)),
    })
  }

  const submitDays = (
    e: React.FormEvent,
    field: 'traceRetentionDays' | 'usageLogRetentionDays',
    raw: string,
    reset: () => void,
  ) => {
    e.preventDefault()
    const n = parseInt(raw, 10)
    if (isNaN(n) || n < 1 || n > 365) {
      toast.error('保留天数需在 1..=365')
      return
    }
    save({ [field]: n }, '保留天数已更新')
    reset()
  }

  return (
    <DropdownMenu open={open} onOpenChange={setOpen}>
      <DropdownMenuTrigger asChild>
        <Button size="sm" variant="outline">
          <Settings2 className="h-3.5 w-3.5" />
          治理设置
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-72">
        <DropdownMenuLabel>请求链路追踪</DropdownMenuLabel>
        <div className="px-2 pb-2">
          <div className="flex items-center justify-between gap-2 rounded-md bg-secondary/40 px-2.5 py-2">
            <div className="text-xs">
              <div className="font-medium text-foreground">
                {enabled ? '已启用' : '已关闭'}
              </div>
              <div className="leading-snug text-muted-foreground">
                {enabled
                  ? '记录每次请求的完整重试链路到 traces.db'
                  : '不再写入新链路（历史记录仍可查询）'}
              </div>
            </div>
            <Switch
              checked={enabled}
              disabled={isLoading || isPending}
              onCheckedChange={(v) =>
                save({ traceEnabled: v }, v ? '已开启链路追踪' : '已关闭链路追踪')
              }
            />
          </div>
        </div>
        <DropdownMenuLabel className="pt-1">
          trace 保留天数（当前 {cfg?.traceRetentionDays ?? '—'}）
        </DropdownMenuLabel>
        <form
          onSubmit={(e) => submitDays(e, 'traceRetentionDays', traceDays, () => setTraceDays(''))}
          className="flex items-center gap-1.5 px-2 pb-2"
        >
          <Input
            type="number"
            min={1}
            max={365}
            placeholder="天数"
            value={traceDays}
            onChange={(e) => setTraceDays(e.target.value)}
            disabled={isPending}
            className="h-7 text-xs"
          />
          <Button type="submit" size="sm" variant="outline" className="h-7 text-xs" disabled={isPending || !traceDays.trim()}>
            保存
          </Button>
        </form>
        <DropdownMenuLabel className="pt-1">
          usage 日志保留天数（当前 {cfg?.usageLogRetentionDays ?? '—'}）
        </DropdownMenuLabel>
        <form
          onSubmit={(e) => submitDays(e, 'usageLogRetentionDays', usageDays, () => setUsageDays(''))}
          className="flex items-center gap-1.5 px-2 pb-2"
        >
          <Input
            type="number"
            min={1}
            max={365}
            placeholder="天数"
            value={usageDays}
            onChange={(e) => setUsageDays(e.target.value)}
            disabled={isPending}
            className="h-7 text-xs"
          />
          <Button type="submit" size="sm" variant="outline" className="h-7 text-xs" disabled={isPending || !usageDays.trim()}>
            保存
          </Button>
        </form>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}


const PAGE_SIZE = 50

export function TraceLogPage() {
  const [operation, setOperation] = useState('')
  const [status, setStatus] = useState('')
  const [errorType, setErrorType] = useState('')
  const [keyId, setKeyId] = useState('')
  const [group, setGroup] = useState('')
  const [onlyFailed, setOnlyFailed] = useState(false)
  const [page, setPage] = useState(0)

  const { data: keysData } = useClientKeys()
  const keyOptions = [
    { value: '', label: '全部 Key' },
    ...(keysData?.keys ?? []).map((k) => ({ value: String(k.id), label: k.name })),
  ]

  const groupOptions = useGroupOptions()
  const groupSelectOptions = [
    { value: '', label: '全部分组' },
    ...groupOptions.map((g) => ({ value: g, label: g })),
  ]

  // 筛选条件变化时回到第一页
  const resetTo = <T,>(setter: (v: T) => void) => (v: T) => {
    setter(v)
    setPage(0)
  }

  const query: TraceQuery = {
    operation: operation || undefined,
    status: status || undefined,
    errorType: errorType || undefined,
    keyId: keyId ? Number(keyId) : undefined,
    group: group || undefined,
    onlyFailed: onlyFailed || undefined,
    limit: PAGE_SIZE,
    offset: page * PAGE_SIZE,
  }
  const { data, isLoading, isFetching, refetch } = useTraces(query)
  const { data: baseline } = usePhaseBaseline(24)
  const records = data?.records ?? []
  const total = data?.total ?? 0
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE))

  return (
    <div className="space-y-5">
      {/* 筛选栏 */}
      <div className="flex flex-wrap items-center gap-3">
        <div className="flex items-center gap-2">
          <ScrollText className="h-5 w-5 text-muted-foreground" />
          <h2 className="text-lg font-semibold tracking-tight">请求日志</h2>
          {total > 0 && <Badge variant="secondary">{total}</Badge>}
        </div>
        <div className="ml-auto flex flex-wrap items-center gap-2">
          <Select value={operation} onChange={resetTo(setOperation)} options={OPERATION_OPTIONS} />
          <Select value={keyId} onChange={resetTo(setKeyId)} options={keyOptions} />
          <Select value={group} onChange={resetTo(setGroup)} options={groupSelectOptions} />
          <Select value={status} onChange={resetTo(setStatus)} options={STATUS_OPTIONS} />
          <Select
            value={errorType}
            onChange={resetTo(setErrorType)}
            options={ERROR_TYPE_OPTIONS}
          />
          <Button
            size="sm"
            variant={onlyFailed ? 'default' : 'outline'}
            onClick={() => {
              setOnlyFailed((v) => !v)
              setPage(0)
            }}
          >
            只看失败
          </Button>
          <GovernanceButton />
          <Button size="sm" variant="outline" onClick={() => refetch()} disabled={isFetching}>
            <RefreshCw className={`h-3.5 w-3.5 ${isFetching ? 'animate-spin' : ''}`} />
            刷新
          </Button>
        </div>
      </div>

      <Card>
        <CardContent className="p-0">
          {isLoading ? (
            <div className="p-6 text-sm text-muted-foreground">加载中…</div>
          ) : records.length === 0 ? (
            <div className="p-6 text-sm text-muted-foreground">
              暂无记录。
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full min-w-[1080px] text-left">
                <thead>
                  <tr className="whitespace-nowrap border-b border-border/60 text-[12px] uppercase tracking-wider text-muted-foreground">
                    <th className="py-2 pl-3 pr-2 font-medium"></th>
                    <th className="py-2 pr-3 font-medium">时间</th>
                    <th className="py-2 pr-3 font-medium">请求</th>
                    <th className="py-2 pr-3 font-medium">入口 Key</th>
                    <th className="py-2 pr-3 font-medium">状态</th>
                    <th className="py-2 pr-3 font-medium">最终凭据</th>
                    <th className="py-2 pr-3 font-medium">Token</th>
                    <th className="py-2 pr-3 font-medium">费用</th>
                    <th className="py-2 pr-3 font-medium" title="首个上游 chunk / 首个可渲染帧">
                      首Token/渲染
                    </th>
                    <th className="py-2 pr-3 font-medium">错误类型</th>
                    <th className="py-2 pr-3 font-medium">重试</th>
                    <th className="py-2 pr-3 font-medium">耗时</th>
                  </tr>
                </thead>
                <tbody>
                  {records.map((rec) => (
                    <TraceRow key={rec.traceId} rec={rec} baseline={baseline} />
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>

      {total > PAGE_SIZE && (
        <div className="flex items-center justify-center gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => setPage((p) => Math.max(0, p - 1))}
            disabled={page === 0 || isFetching}
          >
            <ChevronLeft className="h-3.5 w-3.5" />
            上一页
          </Button>
          <div className="px-3 text-sm tabular-nums text-muted-foreground">
            第 <span className="font-medium text-foreground">{page + 1}</span> /{' '}
            {totalPages} 页
            <span className="mx-1.5 text-muted-foreground/50">·</span>共 {total} 条
          </div>
          <Button
            variant="outline"
            size="sm"
            onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
            disabled={page >= totalPages - 1 || isFetching}
          >
            下一页
            <ChevronRight className="h-3.5 w-3.5" />
          </Button>
        </div>
      )}
    </div>
  )
}


