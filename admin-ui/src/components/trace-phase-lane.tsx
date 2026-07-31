import type { PhaseBaselineRow, TracePhase } from '@/types/api'
import { Badge } from '@/components/ui/badge'
import { formatDuration } from '@/lib/format'

/** 与 trace-timing-bar.tsx 的 PHASE_LABEL 保持一致 */
const PHASE_LABEL: Record<string, string> = {
  first_token: '等首字节',
  streaming: '流传输',
  finish: '收尾',
  body_read: '收 body',
  decode: '解码',
  assemble: '组装',
}

/** 该段是否算失败。client_disconnected 是客户端行为，不算故障。 */
function isFailed(outcome: string) {
  return outcome !== 'success' && outcome !== 'client_disconnected'
}

/** 拼接该段 × 出口的窗口基线；出口 key 与后端 phase_baseline 的 COALESCE(...,'') 对齐 */
function baselineFor(
  rows: PhaseBaselineRow[] | undefined,
  phase: string,
  proxyUrl: string | null | undefined,
) {
  if (!rows) return null
  const key = proxyUrl ?? ''
  const row = rows.find((r) => r.phase === phase && r.proxyUrl === key)
  if (!row || row.total === 0) return null
  return {
    pct: ((row.failed / row.total) * 100).toFixed(1),
    failed: row.failed,
    total: row.total,
  }
}

/** 分段明细：与「尝试链路」（attempts，N 跳重试）并列的第二层，
 * 覆盖 headers 之后的响应处理（流式 first_token/streaming/finish；
 * 非流式 first_token/body_read/decode/assemble）。
 *
 * 与 `TraceTimingBar` 的分工：条形图给比例，这里给条形图放不下的证据——
 * 每段字节数、近 24h 同出口该段失败率（区分「这次异常」还是「该出口一贯如此」）、
 * 错误片段全文。 */
export function TracePhaseLane({
  phases,
  proxyUrl,
  baseline,
}: {
  phases: TracePhase[]
  /** 最终那一跳的出口，用于挑对照基线 */
  proxyUrl: string | null | undefined
  baseline: PhaseBaselineRow[] | undefined
}) {
  if (phases.length === 0) {
    return (
      <div className="text-[12px] text-muted-foreground">
        无分段记录（请求未到达上游，或该记录早于分段埋点）
      </div>
    )
  }
  return (
    <div className="flex flex-wrap gap-2">
      {phases.map((p) => {
        const failed = isFailed(p.outcome)
        const base = baselineFor(baseline, p.phase, proxyUrl)
        return (
          <div
            key={p.seq}
            className={`min-w-[160px] flex-1 rounded-lg border p-2 ${
              failed ? 'border-destructive/50 bg-destructive/5' : 'border-border/50 bg-secondary/30'
            }`}
          >
            <div className="flex items-center gap-2 text-[13px]">
              <span className="font-medium">{PHASE_LABEL[p.phase] ?? p.phase}</span>
              <Badge variant={failed ? 'destructive' : 'secondary'}>{p.outcome}</Badge>
              <span className="ml-auto font-mono text-muted-foreground">
                {formatDuration(p.durationMs)}
              </span>
            </div>
            {p.bytes != null && (
              <div className="mt-1 font-mono text-[11px] text-muted-foreground">
                累计 {p.bytes} B
              </div>
            )}
            {base && (
              <div className="mt-1 text-[11px] text-muted-foreground/80">
                近24h 同出口该段失败率 {base.pct}% ({base.failed}/{base.total})
              </div>
            )}
            {p.detail && (
              <pre className="mt-1 max-h-24 overflow-auto whitespace-pre-wrap break-all rounded bg-background/60 p-1 font-mono text-[11px] text-muted-foreground">
                {p.detail}
              </pre>
            )}
          </div>
        )
      })}
    </div>
  )
}
