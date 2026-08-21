import { useState, useEffect, useMemo } from 'react'
import { toast } from 'sonner'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
  DialogDescription,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import {
  Select,
  SelectTrigger,
  SelectValue,
  SelectContent,
  SelectItem,
} from '@/components/ui/select'
import { getProxyPool, batchAssignProxy, type BatchAssignFailure } from '@/api/credentials'
import {
  type Baseline,
  buildBaseline,
  canSeed,
  diffAssignments,
  isEditable,
  seedSelection,
} from '@/lib/batch-assign-proxy'
import { extractErrorMessage, maskProxyUrl } from '@/lib/utils'
import type { CredentialStatusItem } from '@/types/api'

interface BatchAssignProxyDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  /** 全部凭据（不筛选） */
  credentials: CredentialStatusItem[]
}

const GLOBAL_VALUE = '__global__'

export function BatchAssignProxyDialog({
  open,
  onOpenChange,
  credentials,
}: BatchAssignProxyDialogProps) {
  const queryClient = useQueryClient()

  const { data: proxyPool } = useQuery({
    queryKey: ['proxy-pool'],
    queryFn: getProxyPool,
    enabled: open,
  })

  const selectableProxies = useMemo(
    () => (proxyPool?.proxies ?? []).filter((p) => p.enabled && !p.autoDisabled),
    [proxyPool],
  )

  /**
   * 冻结基线：diff 的唯一参照物，与 selection 同帧同源产生。
   *
   * 不能用 useMemo(credentials, proxyPool) 当基线——那会随异步数据重算：
   * 代理池晚到时基线从「全 custom」翻成真实 id，而 selection 停在旧值，
   * 于是没动过的行被整批判成「要解绑」（首开必现的批量误解绑）；
   * 弹窗开着时 useCredentials 每 30s 轮询，外部改动同样会被算成用户改动。
   */
  const [baseline, setBaseline] = useState<Baseline>(() => new Map())
  const [seeded, setSeeded] = useState(false)
  // 每行当前选值：proxyId（number）或 null（跟随全局）
  const [selection, setSelection] = useState<Record<number, number | null>>({})
  const [failures, setFailures] = useState<Record<number, string>>({})
  const [submitting, setSubmitting] = useState(false)

  useEffect(() => {
    if (!open) {
      // 关闭即重置，下次打开重新播种
      setSeeded(false)
      setBaseline(new Map())
      setSelection({})
      setFailures({})
      setSubmitting(false)
      return
    }
    if (!canSeed(open, proxyPool, seeded)) return
    const next = buildBaseline(credentials, proxyPool!.proxies)
    setBaseline(next)
    setSelection(seedSelection(next))
    setSeeded(true)
    // credentials 刻意不进依赖：播种是一次性的，之后的轮询不得改动基线。
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, proxyPool, seeded])

  const handleChange = (credentialId: number, value: string) => {
    setSelection((prev) => ({
      ...prev,
      [credentialId]: value === GLOBAL_VALUE ? null : Number(value),
    }))
  }

  const changedAssignments = useMemo(
    () => diffAssignments(baseline, selection),
    [baseline, selection],
  )

  // 只渲染基线里有的凭据：播种后新增的凭据用户没见过其初值，不展示也不提交；
  // 已被删除的凭据自然从 credentials 里消失，不再渲染。
  const rows = useMemo(
    () => credentials.filter((c) => baseline.has(c.id)),
    [credentials, baseline],
  )

  const handleSubmit = async () => {
    if (changedAssignments.length === 0) return
    setSubmitting(true)
    setFailures({})
    try {
      const resp = await batchAssignProxy(changedAssignments)
      toast.success(resp.message)
      await queryClient.invalidateQueries({ queryKey: ['credentials'] })
      onOpenChange(false)
    } catch (err) {
      const respFailures = (err as { response?: { data?: { failures?: BatchAssignFailure[] } } })
        ?.response?.data?.failures
      if (respFailures && respFailures.length > 0) {
        const map: Record<number, string> = {}
        for (const f of respFailures) map[f.credentialId] = f.reason
        setFailures(map)
      } else {
        toast.error(`批量重绑失败: ${extractErrorMessage(err)}`)
      }
      // 400 时不关弹窗、不清空已选
    } finally {
      setSubmitting(false)
    }
  }

  const loading = !seeded

  return (
    <Dialog open={open} onOpenChange={(o) => !submitting && onOpenChange(o)}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>批量绑代理（{credentials.length} 张凭据）</DialogTitle>
          <DialogDescription>
            只提交被改动的行；查不到匹配代理的自定义 URL、以及绑在已禁用代理上的行保持只读，不会被覆盖。
          </DialogDescription>
        </DialogHeader>

        <div className="max-h-[60vh] space-y-2 overflow-y-auto py-2">
          {loading ? (
            // 代理池未返回前不渲染凭据行：此时无法判断每行的真实基线
            <p className="py-6 text-center text-sm text-muted-foreground">正在加载代理池…</p>
          ) : (
            rows.map((c) => {
              const initial = baseline.get(c.id)
              // 只读行（自定义 URL / 已禁用代理）：下拉里没有对应项，只展示不提交
              const readOnly = isEditable(initial) ? null : (initial ?? null)
              const label = c.email || `#${c.id}`
              const failure = failures[c.id]

              return (
                <div
                  key={c.id}
                  className="flex items-center gap-3 rounded-xl border border-border/60 p-2.5"
                >
                  <span className="flex-1 truncate text-sm">{label}</span>
                  {readOnly ? (
                    <span
                      className="w-56 truncate text-xs text-muted-foreground"
                      title={readOnly.url}
                    >
                      {readOnly.kind === 'disabled' ? '已禁用代理: ' : '自定义: '}
                      {maskProxyUrl(readOnly.url)}
                    </span>
                  ) : (
                    <div className="w-56 space-y-1">
                      <Select
                        value={selection[c.id] == null ? GLOBAL_VALUE : String(selection[c.id])}
                        onValueChange={(v) => handleChange(c.id, v)}
                        disabled={submitting}
                      >
                        <SelectTrigger className="h-9 rounded-lg px-3 text-sm">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value={GLOBAL_VALUE}>跟随全局代理</SelectItem>
                          {selectableProxies.map((p) => (
                            <SelectItem key={p.id} value={String(p.id)}>
                              {`#${p.id} ${maskProxyUrl(p.url)}（${p.health}, ${p.latencyMs ?? '-'}ms）`}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                      {failure && <p className="text-[11px] text-destructive">{failure}</p>}
                    </div>
                  )}
                </div>
              )
            })
          )}
        </div>

        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)} disabled={submitting}>
            取消
          </Button>
          <Button
            type="button"
            onClick={handleSubmit}
            disabled={loading || submitting || changedAssignments.length === 0}
          >
            {submitting ? '提交中…' : `应用（${changedAssignments.length}）`}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
