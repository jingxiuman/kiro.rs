import { useEffect, useState } from 'react'
import {
  Check,
  Info,
  Lock,
  LockOpen,
  Plus,
  RefreshCw,
  Trash2,
  TriangleAlert,
} from 'lucide-react'
import { toast } from 'sonner'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Switch } from '@/components/ui/switch'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { useConfirm } from '@/components/ui/confirm-dialog'
import {
  useCreateModel,
  useDeleteAlias,
  useDeleteModel,
  useModelRegistry,
  usePatchModel,
  useSetModelSyncSettings,
  useSyncModels,
  useUpsertAlias,
} from '@/hooks/use-model-registry'
import { useCredentials } from '@/hooks/use-credentials'
import { extractErrorMessage, formatNumber } from '@/lib/utils'
import type {
  LockableModelField,
  ModelRegistryResponse,
  ModelRow,
  PatchModelRequest,
} from '@/types/api'

type TabKey = 'models' | 'aliases' | 'sync'

const TABS: { key: TabKey; label: string }[] = [
  { key: 'models', label: '模型表' },
  { key: 'aliases', label: '别名' },
  { key: 'sync', label: '同步设置' },
]

interface ModelMappingDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
}

/**
 * 模型映射弹窗：模型表 / 别名 / 同步设置三个页签。
 *
 * 「锁定」= 后端的 pinned。含义是「这个值以我填的为准，自动同步和版本升级都不会
 * 改它」。只有 5 个会被同步覆盖的字段支持锁定（见 LOCKABLE_MODEL_FIELDS）；
 * 启用开关、排序、匹配方式属于本地策略，同步根本不碰，因此不显示锁。
 */
export function ModelMappingDialog({ open, onOpenChange }: ModelMappingDialogProps) {
  const [tab, setTab] = useState<TabKey>('models')
  const { data, isLoading, error } = useModelRegistry(open)

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-4xl">
        <DialogHeader>
          <DialogTitle>模型映射</DialogTitle>
          <DialogDescription>
            管理对外暴露的模型清单、上游模型的映射关系与自动同步策略。
          </DialogDescription>
        </DialogHeader>

        <div className="flex items-center gap-1 rounded-full border border-border/60 p-0.5 self-start">
          {TABS.map((t) => (
            <button
              key={t.key}
              type="button"
              onClick={() => setTab(t.key)}
              className={
                'rounded-full px-3.5 py-1 text-[13px] transition-colors ' +
                (tab === t.key
                  ? 'bg-primary text-primary-foreground'
                  : 'text-muted-foreground hover:text-foreground')
              }
            >
              {t.label}
            </button>
          ))}
        </div>

        {isLoading && (
          <div className="flex items-center justify-center py-10">
            <div className="h-8 w-8 animate-spin rounded-full border-b-2 border-primary" />
          </div>
        )}

        {error && (
          <div className="py-8 text-center text-sm text-destructive">
            加载模型表失败：{extractErrorMessage(error)}
          </div>
        )}

        {data && (
          <>
            <RegistryBanners data={data} />
            <div className="max-h-[62vh] space-y-2 overflow-auto pr-1">
              {tab === 'models' && <ModelsTab data={data} />}
              {tab === 'aliases' && <AliasesTab data={data} />}
              {tab === 'sync' && <SyncTab data={data} />}
            </div>
          </>
        )}
      </DialogContent>
    </Dialog>
  )
}

/** 顶部状态区：降级横幅、同步时间与「立即同步」、凭据覆盖率提示 */
function RegistryBanners({ data }: { data: ModelRegistryResponse }) {
  const { mutate: sync, isPending } = useSyncModels()

  const runSync = () => {
    sync(undefined, {
      onSuccess: (s) => {
        if (!s.trusted) {
          toast.warning(
            `本轮同步结果不可信（${s.round}），未写入模型表。来源：${s.source}`,
          )
          return
        }
        toast.success(
          `同步完成：新增 ${s.added}，更新 ${s.updated}，标记已下线 ${s.deprecated}`,
        )
      },
      onError: (e) => toast.error(`同步失败：${extractErrorMessage(e)}`),
    })
  }

  const uncovered = data.credentialTotal - data.credentialSupportCovered

  return (
    <div className="space-y-2">
      {data.degraded && (
        <div className="flex items-start gap-2 rounded-lg border border-destructive/40 bg-destructive/10 px-3 py-2 text-xs text-destructive">
          <TriangleAlert className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          <div>
            <div className="font-medium">
              模型配置文件不可用，当前运行在内置默认表上
            </div>
            <div className="mt-0.5 opacity-90">{data.degradedReason}</div>
          </div>
        </div>
      )}

      <div className="flex flex-wrap items-center justify-between gap-2 rounded-lg bg-secondary/40 px-3 py-2">
        <div className="text-xs text-muted-foreground">
          上次同步：
          <span className="text-foreground">
            {formatTime(data.syncState.lastSyncAt) || '从未同步'}
          </span>
          {data.syncState.source && (
            <span className="ml-2">来源：{data.syncState.source}</span>
          )}
        </div>
        <Button size="sm" variant="outline" disabled={isPending} onClick={runSync}>
          <RefreshCw className={'h-3.5 w-3.5 ' + (isPending ? 'animate-spin' : '')} />
          {isPending ? '同步中…' : '立即同步'}
        </Button>
      </div>

      {uncovered > 0 && (
        <div className="flex items-start gap-2 rounded-lg border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-xs text-amber-600 dark:text-amber-400">
          <Info className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          <span>
            {uncovered}/{data.credentialTotal} 个凭据尚未记录可用模型，同步结果可能
            不完整。建议在「同步设置」里指定一个稳定可用的探针凭据。
          </span>
        </div>
      )}
    </div>
  )
}

// ============ 模型表 ============

function ModelsTab({ data }: { data: ModelRegistryResponse }) {
  const [creating, setCreating] = useState(false)

  return (
    <div className="space-y-2">
      <div className="flex items-start justify-between gap-2">
        <p className="text-[11px] leading-snug text-muted-foreground">
          <Lock className="mr-1 inline h-3 w-3 align-[-2px]" />
          带锁的字段表示「以我填的值为准」，自动同步与版本升级都不会覆盖它；点击锁
          图标可以解除，让该字段重新跟随上游。修改一个字段会自动锁上它。
        </p>
        <Button
          size="sm"
          variant="outline"
          className="shrink-0"
          onClick={() => setCreating((v) => !v)}
        >
          <Plus className="h-3.5 w-3.5" />
          手动新增
        </Button>
      </div>

      {creating && <CreateModelForm onDone={() => setCreating(false)} />}

      {data.models.map((row) => (
        <ModelRowCard key={row.upstreamId} row={row} />
      ))}
    </div>
  )
}

function ModelRowCard({ row }: { row: ModelRow }) {
  const { mutate: patch, isPending } = usePatchModel()
  const { mutate: remove, isPending: isRemoving } = useDeleteModel()
  const confirm = useConfirm()

  const apply = (p: PatchModelRequest) => {
    patch(
      { upstreamId: row.upstreamId, patch: p },
      {
        onSuccess: () => toast.success('已保存'),
        onError: (e) => toast.error(`保存失败：${extractErrorMessage(e)}`),
      },
    )
  }

  const handleDelete = async () => {
    const ok = await confirm({
      title: '删除模型',
      description: `确定从映射表中删除 ${row.exposedId}（上游 ${row.upstreamId}）吗？`,
      destructive: true,
      confirmText: '删除',
    })
    if (!ok) return
    remove(row.upstreamId, {
      onSuccess: () => toast.success('已删除'),
      onError: (e) => toast.error(`删除失败：${extractErrorMessage(e)}`),
    })
  }

  const busy = isPending || isRemoving

  return (
    <div className="rounded-lg border border-border/60 bg-secondary/30 px-3 py-2.5">
      <div className="flex flex-wrap items-center gap-2">
        <TextField
          className="min-w-[200px] flex-1"
          field="exposedId"
          row={row}
          value={row.exposedId}
          disabled={busy}
          onApply={apply}
          mono
        />
        <StatusBadges row={row} />
        <div className="flex items-center gap-1.5">
          <span className="text-[11px] text-muted-foreground">启用</span>
          <Switch
            checked={row.enabled}
            disabled={busy}
            onCheckedChange={(v) => apply({ enabled: v })}
          />
        </div>
        {row.origin !== 'builtin' && (
          <Button
            size="icon"
            variant="ghost"
            className="h-7 w-7"
            title="删除该模型"
            disabled={busy}
            onClick={handleDelete}
          >
            <Trash2 className="h-3.5 w-3.5 text-destructive" />
          </Button>
        )}
      </div>

      <div className="mt-1 font-mono text-[11px] text-muted-foreground">
        上游 ID：{row.upstreamId}
      </div>

      <div className="mt-2 grid gap-2 sm:grid-cols-2">
        <TextField
          field="displayName"
          label="显示名"
          row={row}
          value={row.displayName}
          disabled={busy}
          onApply={apply}
        />
        <div className="flex items-center gap-2">
          <label className="w-24 shrink-0 text-[11px] text-muted-foreground">
            思考变体
          </label>
          <Switch
            checked={row.exposeThinkingVariant}
            disabled={busy}
            onCheckedChange={(v) => apply({ exposeThinkingVariant: v })}
          />
          <span className="text-[11px] text-muted-foreground">
            额外暴露一个 -thinking 版本
          </span>
          <LockButton
            field="exposeThinkingVariant"
            row={row}
            disabled={busy}
            onApply={apply}
            currentValue={{ exposeThinkingVariant: row.exposeThinkingVariant }}
          />
        </div>
        <NumberField
          field="contextWindow"
          label="输入窗口"
          hint="单次请求可携带的最大输入 token 数"
          row={row}
          value={row.contextWindow}
          disabled={busy}
          onApply={apply}
        />
        <NumberField
          field="maxOutputTokens"
          label="输出上限"
          hint="单次回复可生成的最大 token 数"
          row={row}
          value={row.maxOutputTokens}
          disabled={busy}
          onApply={apply}
        />
      </div>

      {row.status === 'deprecated' && (
        <div className="mt-2 text-[11px] text-amber-600 dark:text-amber-400">
          上游已连续 {row.missingSyncRounds} 轮未返回该模型
          {row.lastSeenAt && `，最后一次出现于 ${formatTime(row.lastSeenAt)}`}
          。仍可继续调用。
        </div>
      )}
    </div>
  )
}

function StatusBadges({ row }: { row: ModelRow }) {
  return (
    <div className="flex flex-wrap items-center gap-1">
      <Badge variant={row.origin === 'builtin' ? 'secondary' : 'outline'}>
        {ORIGIN_LABEL[row.origin]}
      </Badge>
      {row.status === 'deprecated' && <Badge variant="warning">上游已下线</Badge>}
      {row.matchKind === 'prefix' && <Badge variant="outline">前缀匹配</Badge>}
      {!row.listed && <Badge variant="outline">不在模型列表中</Badge>}
    </div>
  )
}

const ORIGIN_LABEL: Record<ModelRow['origin'], string> = {
  builtin: '内置',
  synced: '同步',
  manual: '手动',
}

/**
 * 锁图标按钮。
 *
 * 已锁 → 点击发 `unpin`，字段回归自动同步；
 * 未锁 → 点击用**当前值**发一次 PATCH，后端会自动把它加进锁定集。
 */
function LockButton({
  field,
  row,
  disabled,
  currentValue,
  onApply,
}: {
  field: LockableModelField
  row: ModelRow
  disabled?: boolean
  /** 「锁上」时要回写的当前值 */
  currentValue: PatchModelRequest
  onApply: (patch: PatchModelRequest) => void
}) {
  const locked = row.pinned.includes(field)
  return (
    <button
      type="button"
      disabled={disabled}
      title={
        locked
          ? '已锁定：自动同步不会覆盖这个值。点击解除锁定，让它重新跟随上游'
          : '未锁定：自动同步可能覆盖这个值。点击锁定，保留当前值'
      }
      onClick={() => onApply(locked ? { unpin: [field] } : currentValue)}
      className={
        'shrink-0 rounded p-1 transition-colors disabled:opacity-50 ' +
        (locked
          ? 'text-primary hover:bg-primary/10'
          : 'text-muted-foreground/50 hover:text-foreground')
      }
    >
      {locked ? <Lock className="h-3.5 w-3.5" /> : <LockOpen className="h-3.5 w-3.5" />}
    </button>
  )
}

/** 失焦或回车提交的文本字段，右侧带锁按钮 */
function TextField({
  field,
  label,
  row,
  value,
  disabled,
  className,
  mono,
  onApply,
}: {
  field: Extract<LockableModelField, 'exposedId' | 'displayName'>
  label?: string
  row: ModelRow
  value: string
  disabled?: boolean
  className?: string
  mono?: boolean
  onApply: (patch: PatchModelRequest) => void
}) {
  const [draft, setDraft] = useState(value)
  useEffect(() => setDraft(value), [value])

  const commit = () => {
    const next = draft.trim()
    if (!next) {
      setDraft(value)
      toast.error('不能为空')
      return
    }
    if (next === value) return
    onApply({ [field]: next })
  }

  return (
    <div className={'flex items-center gap-1.5 ' + (className ?? '')}>
      {label && (
        <label className="w-24 shrink-0 text-[11px] text-muted-foreground">
          {label}
        </label>
      )}
      <Input
        value={draft}
        disabled={disabled}
        onChange={(e) => setDraft(e.target.value)}
        onBlur={commit}
        onKeyDown={(e) => {
          if (e.key === 'Enter') e.currentTarget.blur()
          if (e.key === 'Escape') setDraft(value)
        }}
        className={'h-7 text-xs ' + (mono ? 'font-mono' : '')}
      />
      <LockButton
        field={field}
        row={row}
        disabled={disabled}
        currentValue={{ [field]: value }}
        onApply={onApply}
      />
    </div>
  )
}

/** 失焦或回车提交的正整数字段，右侧带锁按钮 */
function NumberField({
  field,
  label,
  hint,
  row,
  value,
  disabled,
  onApply,
}: {
  field: Extract<LockableModelField, 'contextWindow' | 'maxOutputTokens'>
  label: string
  hint: string
  row: ModelRow
  value: number
  disabled?: boolean
  onApply: (patch: PatchModelRequest) => void
}) {
  const [draft, setDraft] = useState(String(value))
  useEffect(() => setDraft(String(value)), [value])

  const commit = () => {
    const next = Number(draft.trim())
    if (!Number.isInteger(next) || next <= 0) {
      setDraft(String(value))
      toast.error(`${label}必须是正整数`)
      return
    }
    if (next === value) return
    onApply({ [field]: next })
  }

  return (
    <div className="flex items-center gap-1.5">
      <label className="w-24 shrink-0 text-[11px] text-muted-foreground" title={hint}>
        {label}
      </label>
      <Input
        type="number"
        min={1}
        value={draft}
        disabled={disabled}
        onChange={(e) => setDraft(e.target.value)}
        onBlur={commit}
        onKeyDown={(e) => {
          if (e.key === 'Enter') e.currentTarget.blur()
          if (e.key === 'Escape') setDraft(String(value))
        }}
        className="h-7 text-xs tabular-nums"
        title={hint}
      />
      <span className="w-10 shrink-0 text-[11px] tabular-nums text-muted-foreground">
        {formatNumber(value)}
      </span>
      <LockButton
        field={field}
        row={row}
        disabled={disabled}
        currentValue={{ [field]: value }}
        onApply={onApply}
      />
    </div>
  )
}

function CreateModelForm({ onDone }: { onDone: () => void }) {
  const [upstreamId, setUpstreamId] = useState('')
  const { mutate: create, isPending } = useCreateModel()

  const submit = (e: React.FormEvent) => {
    e.preventDefault()
    const id = upstreamId.trim()
    if (!id) {
      toast.error('请填写上游模型 ID')
      return
    }
    create(
      { upstreamId: id },
      {
        onSuccess: () => {
          toast.success('已加入映射表')
          setUpstreamId('')
          onDone()
        },
        onError: (e2) => toast.error(`新增失败：${extractErrorMessage(e2)}`),
      },
    )
  }

  return (
    <form
      onSubmit={submit}
      className="flex items-center gap-2 rounded-lg border border-border/60 bg-card/60 px-3 py-2"
    >
      <Input
        autoFocus
        placeholder="上游模型 ID，例如 claude-opus-4.8"
        value={upstreamId}
        disabled={isPending}
        onChange={(e) => setUpstreamId(e.target.value)}
        className="h-8 font-mono text-xs"
      />
      <Button type="submit" size="sm" disabled={isPending || !upstreamId.trim()}>
        新增
      </Button>
      <Button type="button" size="sm" variant="outline" onClick={onDone}>
        取消
      </Button>
    </form>
  )
}

// ============ 别名 ============

function AliasesTab({ data }: { data: ModelRegistryResponse }) {
  const [from, setFrom] = useState('')
  const [to, setTo] = useState('')
  const { mutate: upsert, isPending } = useUpsertAlias()
  const { mutate: remove, isPending: isRemoving } = useDeleteAlias()

  const submit = (e: React.FormEvent) => {
    e.preventDefault()
    const f = from.trim()
    if (!f || !to) {
      toast.error('请填写别名并选择目标模型')
      return
    }
    upsert(
      { from: f, to },
      {
        onSuccess: () => {
          toast.success('别名已保存')
          setFrom('')
          setTo('')
        },
        onError: (err) => toast.error(`保存失败：${extractErrorMessage(err)}`),
      },
    )
  }

  return (
    <div className="space-y-2">
      <p className="text-[11px] leading-snug text-muted-foreground">
        别名让客户端用一个短名字（如 opus）请求某个上游模型。别名只属于人工配置，
        自动同步不会改动它。
      </p>

      <form
        onSubmit={submit}
        className="flex flex-wrap items-center gap-2 rounded-lg border border-border/60 bg-card/60 px-3 py-2"
      >
        <Input
          placeholder="别名，例如 opus"
          value={from}
          disabled={isPending}
          onChange={(e) => setFrom(e.target.value)}
          className="h-8 w-40 font-mono text-xs"
        />
        <span className="text-xs text-muted-foreground">指向</span>
        <Select value={to} onValueChange={setTo} disabled={isPending}>
          <SelectTrigger className="h-8 w-64 text-xs">
            <SelectValue placeholder="选择目标上游模型" />
          </SelectTrigger>
          <SelectContent>
            {data.models.map((m) => (
              <SelectItem key={m.upstreamId} value={m.upstreamId}>
                {m.upstreamId}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Button type="submit" size="sm" disabled={isPending}>
          <Plus className="h-3.5 w-3.5" />
          添加
        </Button>
      </form>

      {data.aliases.length === 0 ? (
        <div className="py-8 text-center text-sm text-muted-foreground">
          还没有配置别名
        </div>
      ) : (
        data.aliases.map((a) => (
          <div
            key={a.from}
            className="flex items-center justify-between gap-2 rounded-lg border border-border/60 bg-secondary/30 px-3 py-2"
          >
            <div className="font-mono text-xs">
              <span className="text-foreground">{a.from}</span>
              <span className="mx-2 text-muted-foreground">→</span>
              <span className="text-muted-foreground">{a.to}</span>
            </div>
            <Button
              size="icon"
              variant="ghost"
              className="h-7 w-7"
              title="删除别名"
              disabled={isRemoving}
              onClick={() =>
                remove(a.from, {
                  onSuccess: () => toast.success('别名已删除'),
                  onError: (err) =>
                    toast.error(`删除失败：${extractErrorMessage(err)}`),
                })
              }
            >
              <Trash2 className="h-3.5 w-3.5 text-destructive" />
            </Button>
          </div>
        ))
      )}
    </div>
  )
}

// ============ 同步设置 ============

const NO_PROBE = '__none__'

function SyncTab({ data }: { data: ModelRegistryResponse }) {
  const { settings } = data
  const { mutate: save, isPending } = useSetModelSyncSettings()
  const { data: credentials } = useCredentials()
  const [time, setTime] = useState(settings.time)

  useEffect(() => setTime(settings.time), [settings.time])

  const apply = (
    patch: Parameters<typeof save>[0],
    okMessage = '已保存',
  ) => {
    save(patch, {
      onSuccess: () => toast.success(okMessage),
      onError: (e) => toast.error(`保存失败：${extractErrorMessage(e)}`),
    })
  }

  return (
    <div className="space-y-2">
      <SettingRow
        title="每日自动同步"
        desc="每天定时从上游拉取一次模型清单，更新未锁定的字段"
      >
        <Switch
          checked={settings.enabled}
          disabled={isPending}
          onCheckedChange={(v) => apply({ enabled: v })}
        />
      </SettingRow>

      <SettingRow title="同步时间" desc="24 小时制，格式 HH:MM，按服务器本地时区">
        <form
          className="flex items-center gap-1.5"
          onSubmit={(e) => {
            e.preventDefault()
            apply({ time })
          }}
        >
          <Input
            value={time}
            disabled={isPending || !settings.enabled}
            onChange={(e) => setTime(e.target.value)}
            placeholder="03:30"
            className="h-7 w-20 text-center text-xs tabular-nums"
          />
          <Button
            type="submit"
            size="sm"
            variant="outline"
            className="h-7 text-xs"
            disabled={isPending || !settings.enabled || time === settings.time}
          >
            <Check className="h-3.5 w-3.5" />
            保存
          </Button>
        </form>
      </SettingRow>

      <SettingRow
        title="探针凭据"
        desc="指定一个稳定可用的凭据作为同步数据源。不指定时随机抽样多个凭据，结果可能不完整"
      >
        <Select
          value={
            settings.probeCredentialId == null
              ? NO_PROBE
              : String(settings.probeCredentialId)
          }
          disabled={isPending}
          onValueChange={(v) =>
            apply({
              probeCredentialIdSet: true,
              probeCredentialId: v === NO_PROBE ? null : Number(v),
            })
          }
        >
          <SelectTrigger className="h-7 w-48 text-xs">
            <SelectValue placeholder="不指定" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value={NO_PROBE}>不指定（随机抽样）</SelectItem>
            {(credentials?.credentials ?? [])
              .filter((c) => !c.disabled)
              .map((c) => (
                <SelectItem key={c.id} value={String(c.id)}>
                  #{c.id} {c.email ?? ''}
                </SelectItem>
              ))}
          </SelectContent>
        </Select>
      </SettingRow>

      <SettingRow
        title="允许未收录模型透传"
        desc="开启后，映射表里没有的模型名也会原样转发给上游；关闭则直接拒绝"
      >
        <Switch
          checked={settings.allowPassthrough}
          disabled={isPending}
          onCheckedChange={(v) => apply({ allowPassthrough: v })}
        />
      </SettingRow>
    </div>
  )
}

function SettingRow({
  title,
  desc,
  children,
}: {
  title: string
  desc: string
  children: React.ReactNode
}) {
  return (
    <div className="flex items-center justify-between gap-4 rounded-lg border border-border/60 bg-secondary/30 px-3 py-2.5">
      <div className="min-w-0">
        <div className="text-[13px] font-medium">{title}</div>
        <div className="mt-0.5 text-[11px] leading-snug text-muted-foreground">
          {desc}
        </div>
      </div>
      <div className="shrink-0">{children}</div>
    </div>
  )
}

// ============ 工具 ============

function formatTime(iso: string | null | undefined): string {
  if (!iso) return ''
  const d = new Date(iso)
  if (Number.isNaN(d.getTime())) return iso
  return d.toLocaleString('zh-CN', { hour12: false })
}
