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
import { getProxyPool, batchAssignProxy, type BatchAssignEntry, type BatchAssignFailure } from '@/api/credentials'
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

  // 每行当前选值：proxyId（number）或 null（跟随全局）
  const [selection, setSelection] = useState<Record<number, number | null>>({})
  const [failures, setFailures] = useState<Record<number, string>>({})
  const [submitting, setSubmitting] = useState(false)

  // 凭据当前 proxyUrl 反查代理池 id；查不到则视为自定义（该行只读）
  const initialAssignment = useMemo(() => {
    const map = new Map<number, number | null | 'custom'>()
    for (const c of credentials) {
      if (!c.proxyUrl) {
        map.set(c.id, null)
        continue
      }
      const match = (proxyPool?.proxies ?? []).find((p) => p.url === c.proxyUrl)
      map.set(c.id, match ? match.id : 'custom')
    }
    return map
  }, [credentials, proxyPool])

  useEffect(() => {
    if (open) {
      const init: Record<number, number | null> = {}
      for (const c of credentials) {
        const v = initialAssignment.get(c.id)
        init[c.id] = v === 'custom' ? null : (v ?? null)
      }
      setSelection(init)
      setFailures({})
      setSubmitting(false)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open])

  const handleChange = (credentialId: number, value: string) => {
    setSelection((prev) => ({
      ...prev,
      [credentialId]: value === GLOBAL_VALUE ? null : Number(value),
    }))
  }

  const changedAssignments: BatchAssignEntry[] = useMemo(() => {
    const out: BatchAssignEntry[] = []
    for (const c of credentials) {
      const initial = initialAssignment.get(c.id)
      if (initial === 'custom') continue
      const current = selection[c.id] ?? null
      const initialValue = initial ?? null
      if (current !== initialValue) {
        out.push({ credentialId: c.id, proxyId: current })
      }
    }
    return out
  }, [credentials, initialAssignment, selection])

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

  return (
    <Dialog open={open} onOpenChange={(o) => !submitting && onOpenChange(o)}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>批量绑代理（{credentials.length} 张凭据）</DialogTitle>
          <DialogDescription>
            只提交被改动的行；查不到匹配代理的自定义 URL 行保持只读，不会被覆盖。
          </DialogDescription>
        </DialogHeader>

        <div className="max-h-[60vh] space-y-2 overflow-y-auto py-2">
          {credentials.map((c) => {
            const initial = initialAssignment.get(c.id)
            const isCustom = initial === 'custom'
            const label = c.email || `#${c.id}`
            const value = isCustom
              ? undefined
              : selection[c.id] == null
                ? GLOBAL_VALUE
                : String(selection[c.id])
            const failure = failures[c.id]

            return (
              <div
                key={c.id}
                className="flex items-center gap-3 rounded-xl border border-border/60 p-2.5"
              >
                <span className="flex-1 truncate text-sm">{label}</span>
                {isCustom ? (
                  <span className="w-56 truncate text-xs text-muted-foreground" title={c.proxyUrl}>
                    自定义: {maskProxyUrl(c.proxyUrl ?? '')}
                  </span>
                ) : (
                  <div className="w-56 space-y-1">
                    <Select
                      value={value}
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
          })}
        </div>

        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => onOpenChange(false)} disabled={submitting}>
            取消
          </Button>
          <Button
            type="button"
            onClick={handleSubmit}
            disabled={submitting || changedAssignments.length === 0}
          >
            {submitting ? '提交中…' : `应用（${changedAssignments.length}）`}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
