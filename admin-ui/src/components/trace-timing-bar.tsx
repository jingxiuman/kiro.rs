import type { TraceRecord } from '@/types/api'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import { formatDuration } from '@/lib/format'

/** 段名 → 中文标签。流式与非流式共用 first_token。 */
const PHASE_LABEL: Record<string, string> = {
  first_token: '等首字节',
  streaming: '流传输',
  finish: '收尾',
  body_read: '收 body',
  decode: '解码',
  assemble: '组装',
}

/**
 * 段名 → 配色。按"时间花在哪类事情上"分色，而不是按段名逐个配色：
 * 等上游（琥珀）/ 传数据（绿）/ 本地处理（紫、灰蓝）—— 扫一眼就知道瓶颈在谁身上。
 */
const PHASE_COLOR: Record<string, string> = {
  first_token: 'bg-amber-400 dark:bg-amber-500',
  streaming: 'bg-emerald-500',
  body_read: 'bg-emerald-500',
  decode: 'bg-violet-500',
  finish: 'bg-slate-400 dark:bg-slate-500',
  assemble: 'bg-slate-400 dark:bg-slate-500',
}

const ATTEMPT_COLOR = 'bg-sky-500'
const FAILED_COLOR = 'bg-rose-500'
const GAP_COLOR = 'bg-zinc-300/70 dark:bg-zinc-600/70'
const UNKNOWN_COLOR = 'bg-muted-foreground/20'

/** 该段/该跳是否算失败。client_disconnected 是客户端行为，不算故障。 */
function isFailed(outcome: string) {
  return outcome !== 'success' && outcome !== 'client_disconnected'
}

interface RawSegment {
  key: string
  label: string
  startedMs: number
  durationMs: number
  color: string
  note?: string
  failed: boolean
  isAttempt: boolean
}

/** 参与布局的段：`widthMs` 是钳位后的可见宽度，`durationMs` 仍是实测值 */
interface LaidOutSegment extends RawSegment {
  widthMs: number
  /** 实测时长被钳位过（与前一段重叠），tooltip 需注明 */
  clamped: boolean
}

function rawSegments(rec: TraceRecord): { segs: RawSegment[]; estimated: boolean } {
  const attempts = rec.attempts ?? []
  const estimated = attempts.some((a) => a.startedMs == null)
  const segs: RawSegment[] = []
  // startedMs 缺失（该列存在前的历史行）时退化为顺序堆叠，由调用方标注"位置为推算"，
  // 不静默把推算值当实测值展示。
  let cursor = 0
  attempts.forEach((a) => {
    const started = a.startedMs ?? cursor
    const failed = isFailed(a.outcome)
    segs.push({
      key: `attempt-${a.attempt}`,
      label: `第 ${a.attempt + 1} 跳`,
      startedMs: started,
      durationMs: a.durationMs,
      color: failed ? FAILED_COLOR : ATTEMPT_COLOR,
      note: `取凭据 → 建连 → 响应头｜${a.outcome}${
        a.httpStatus != null ? `｜HTTP ${a.httpStatus}` : ''
      }`,
      failed,
      isAttempt: true,
    })
    cursor = started + a.durationMs
  })
  ;(rec.phases ?? []).forEach((p) => {
    const failed = isFailed(p.outcome)
    segs.push({
      key: `phase-${p.seq}`,
      label: PHASE_LABEL[p.phase] ?? p.phase,
      startedMs: p.startedMs,
      durationMs: p.durationMs,
      color: failed ? FAILED_COLOR : (PHASE_COLOR[p.phase] ?? UNKNOWN_COLOR),
      note: failed ? p.outcome : undefined,
      failed,
      isAttempt: false,
    })
  })
  segs.sort((a, b) => a.startedMs - b.startedMs)
  return { segs, estimated }
}

/**
 * 把散落的区间铺成一条不重叠、不留白的时间线。
 *
 * 必须走这一遍游标而不是直接按 durationMs 排 flex：段来自两套独立埋点
 * （attempt 在 provider 侧计时，phase 在 handler 侧计时），毫秒截断与边界重合
 * 都可能让两段轻微交叠。直接排的话宽度之和会超过总时长，flex 收缩把所有段
 * 等比压扁——长段的占比就被读错了，而占比正是这条图唯一要传达的信息。
 *
 * 空隙同理：重试 backoff 落在 record.durationMs 里却不属于任何一跳，不显式
 * 画出来的话，"这条链路有多少时间说不清"就被悄悄抹掉了。
 */
function layout(segs: RawSegment[], total: number): LaidOutSegment[] {
  const out: LaidOutSegment[] = []
  let cursor = 0
  segs.forEach((s, i) => {
    if (s.startedMs > cursor) {
      const prev = segs[i - 1]
      // 两跳之间的洞 = 重试前的 backoff 退避；其余位置的洞是埋点没覆盖到的区间
      const isBackoff = prev?.isAttempt === true && s.isAttempt
      out.push({
        key: `gap-${i}`,
        label: isBackoff ? '重试等待' : '未归因',
        startedMs: cursor,
        durationMs: s.startedMs - cursor,
        widthMs: s.startedMs - cursor,
        color: isBackoff ? GAP_COLOR : UNKNOWN_COLOR,
        note: isBackoff
          ? '重试前的 backoff 退避'
          : '埋点未覆盖的区间：请求转换、token 估算、缓存计量等准备工作',
        failed: false,
        isAttempt: false,
        clamped: false,
      })
      cursor = s.startedMs
    }
    const end = s.startedMs + s.durationMs
    // 0ms 段与被前一段完全吞掉的段：widthMs 记 0 而**不丢弃**。
    // 丢弃过会导致真实发生过的段（例如 0ms 的 finish、或落在前一跳区间里的
    // 失败段）从条形和图例里一并消失——组件本该暴露这些段，不能因为它太短就藏掉。
    // 宽度交给 barWidths 垫到最小可见值。
    const widthMs = Math.max(0, end - cursor)
    out.push({ ...s, widthMs, clamped: widthMs < s.durationMs })
    if (end > cursor) cursor = end
  })
  if (total > cursor) {
    // 只要有剩余就补 tail，不设 1ms 阈值：阈值会让条形和小于 100%，
    // 末尾露出一段无标签、无 tooltip 的空轨道——看起来像渲染坏了，
    // 而不像"这段时间没归因"。短请求（总时长几 ms）时这段空白占比尤其刺眼。
    out.push({
      key: 'tail',
      label: '未归因',
      startedMs: cursor,
      durationMs: total - cursor,
      widthMs: total - cursor,
      color: UNKNOWN_COLOR,
      note: '埋点未覆盖的区间：响应组装收尾、落库等',
      failed: false,
      isAttempt: false,
      clamped: false,
    })
  }
  return out
}

/** 极短段的最小可见宽度（百分比）。低于约 0.4% 时视觉上已不足一像素、也无法 hover。 */
const MIN_VISIBLE_PCT = 0.5

/**
 * 给极短段垫到最小可见宽度，并把垫出来的量从"宽到垫得起"的段里按比例扣回，
 * 保证总和仍是 100%。
 *
 * 不这样做的后果是实测出来的：垫高会让总和超过 100%，父容器 `overflow-hidden`
 * 从尾部裁掉超出部分——末段（往往正是"未归因"）在条上直接看不见，而图例里还
 * 列着它。一个"本该暴露说不清的时间"的设计，因为布局细节把它藏了起来。
 */
function barWidths(rawPcts: number[]): number[] {
  if (rawPcts.length === 0) return []
  const out = rawPcts.map((p) => Math.max(p, MIN_VISIBLE_PCT))
  const sum = out.reduce((s, p) => s + p, 0)
  const debt = sum - 100
  if (Math.abs(debt) < 1e-9) return out
  if (debt < 0) {
    // 总和不足 100%（例如尾隙被并入、或浮点误差）：把差额补给最宽的那段。
    // 补给最宽段而非平摊，是为了不让任何窄段的占比被明显放大。
    const widest = out.reduce((bi, p, i) => (p > out[bi] ? i : bi), 0)
    out[widest] -= debt
    return out
  }
  // 超出 100%：只从高于下限的段扣，且扣完不得跌破下限；按"可扣余量"比例分摊
  const slack = out.map((p) => Math.max(0, p - MIN_VISIBLE_PCT))
  const totalSlack = slack.reduce((s, v) => s + v, 0)
  if (totalSlack <= 0) {
    // 段数多到连下限都放不下（约 200 段）。此时等比压缩全部段：
    // 保证总和为 100%，代价是所有段都低于最小可见宽度。不能原样返回——
    // flexShrink 为 0，超出部分会被父容器从尾部裁掉，末段直接看不见。
    return out.map((p) => (p * 100) / sum)
  }
  return out.map((p, i) => p - (debt * slack[i]) / totalSlack)
}

/**
 * 耗时分布条：把一次请求的总时长拆成色块，按 `durationMs` 归一化。
 *
 * 两层拼接：attempt（取凭据→响应头，N 跳含重试与 backoff 空隙）+ phase
 * （响应头之后的处理）。流式与非流式共用这一套渲染，差别只在 phase 段名——
 * 非流式自 0.8.7 起也有 first_token/body_read/decode/assemble 四段。
 */
export function TraceTimingBar({ rec }: { rec: TraceRecord }) {
  const total = rec.durationMs
  const { segs, estimated } = rawSegments(rec)

  if (total <= 0 || segs.length === 0) {
    return (
      <div className="text-[12px] text-muted-foreground">
        无耗时分段（请求未到达上游）
      </div>
    )
  }

  const laid = layout(segs, total)
  const pct = (ms: number) => (ms / total) * 100
  const widths = barWidths(laid.map((s) => pct(s.widthMs)))

  // layout 理论上总会补出 tail，这里兜住"一段都没铺出来"的意外，
  // 避免渲染一根没有任何色块的空条（看起来像坏了，而不像没数据）。
  if (laid.length === 0) {
    return (
      <div className="text-[12px] text-muted-foreground">
        无法铺出耗时分段（分段数据与总时长不一致）
      </div>
    )
  }

  return (
    <TooltipProvider delayDuration={100}>
      <div className="space-y-2">
        <div className="flex items-center gap-2">
          <div className="flex h-3 flex-1 overflow-hidden rounded-full bg-secondary">
            {laid.map((s, i) => (
              <Tooltip key={s.key}>
                <TooltipTrigger asChild>
                  <div
                    className={`${s.color} h-full cursor-default border-r border-background/40 last:border-r-0`}
                    style={{
                      // 宽度已由 barWidths 垫过下限并配平到 100%，此处不再二次收缩，
                      // 否则钳位算出的比例会被 flex 再压一遍。
                      flexBasis: `${widths[i]}%`,
                      flexShrink: 0,
                    }}
                  />
                </TooltipTrigger>
                <TooltipContent className="p-0">
                  <div className="min-w-[160px] px-3 py-2 text-[12px]">
                    <div className="mb-1 text-[13px] font-semibold">{s.label}</div>
                    <div className="flex items-center justify-between gap-6">
                      <span className="text-muted-foreground">耗时</span>
                      <span className="font-mono tabular-nums">
                        {formatDuration(s.durationMs)}
                      </span>
                    </div>
                    <div className="flex items-center justify-between gap-6">
                      <span className="text-muted-foreground">占比</span>
                      <span className="font-mono tabular-nums">
                        {pct(s.durationMs).toFixed(1)}%
                      </span>
                    </div>
                    <div className="flex items-center justify-between gap-6">
                      <span className="text-muted-foreground">起点</span>
                      <span className="font-mono tabular-nums">
                        +{formatDuration(s.startedMs)}
                      </span>
                    </div>
                    {s.clamped && (
                      <div className="mt-1 text-[11px] text-muted-foreground">
                        与前一段有 {formatDuration(s.durationMs - s.widthMs)} 重叠，
                        条上按不重叠部分绘制
                      </div>
                    )}
                    {s.note && (
                      <div className="mt-1 max-w-[240px] text-[11px] text-muted-foreground">
                        {s.note}
                      </div>
                    )}
                  </div>
                </TooltipContent>
              </Tooltip>
            ))}
          </div>
          <span className="w-14 shrink-0 text-right font-mono text-[12px] tabular-nums text-muted-foreground">
            {formatDuration(total)}
          </span>
        </div>
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-[11px]">
          {laid.map((s) => (
            <span key={`lg-${s.key}`} className="inline-flex items-center gap-1.5">
              <span className={`h-2 w-2 shrink-0 rounded-full ${s.color}`} />
              <span className={s.failed ? 'text-destructive' : 'text-muted-foreground'}>
                {s.label}
              </span>
              <span className="font-mono tabular-nums text-muted-foreground/80">
                {formatDuration(s.durationMs)}
              </span>
            </span>
          ))}
        </div>
        {estimated && (
          <div className="text-[11px] text-muted-foreground/70">
            该记录早于每跳起点埋点，跳的位置由耗时顺序推算，重试等待可能未体现
          </div>
        )}
      </div>
    </TooltipProvider>
  )
}
