import type { BatchAssignEntry } from '@/api/credentials'
import type { CredentialStatusItem, ProxyPoolEntry } from '@/types/api'

/**
 * 一行凭据在「打开弹窗那一刻」的绑定形态。
 *
 * 这是提交体 diff 的**唯一**参照物：一旦播种就冻结，不随 useCredentials 的
 * 30s 轮询或代理池刷新而变。否则外部改动会被误判成用户改动
 * （见 batch-assign-proxy-dialog 的基线竞态）。
 */
export type BaselineValue =
  | number // 绑在池内某个可选（enabled 且非 autoDisabled）代理上
  | null // 跟随全局代理
  | { kind: 'custom'; url: string } // 代理池里找不到该 URL：只读
  | { kind: 'disabled'; url: string } // 池内有该 URL，但已禁用/被健康检查自动禁用：只读

export type Baseline = Map<number, BaselineValue>

/** 可编辑的行（下拉可选）：数字 id 或 null。只读行返回 false。 */
export function isEditable(v: BaselineValue | undefined): v is number | null {
  return v === null || typeof v === 'number'
}

/**
 * 把凭据当前的 proxyUrl 反查代理池，得到冻结基线。
 *
 * **必须在代理池已返回后调用**：池为空时所有带 proxyUrl 的行都会落到
 * `custom`，基线整体失真。调用方用 [`canSeed`] 把关。
 */
export function buildBaseline(
  credentials: CredentialStatusItem[],
  proxies: ProxyPoolEntry[],
): Baseline {
  const map: Baseline = new Map()
  for (const c of credentials) {
    if (!c.proxyUrl) {
      map.set(c.id, null)
      continue
    }
    const match = proxies.find((p) => p.url === c.proxyUrl)
    if (!match) {
      map.set(c.id, { kind: 'custom', url: c.proxyUrl })
    } else if (!match.enabled || match.autoDisabled) {
      // 下拉只列可选代理，这类绑定在下拉里找不到对应项会渲染成空白；
      // 与 custom 同样按只读展示，不进提交体。
      map.set(c.id, { kind: 'disabled', url: c.proxyUrl })
    } else {
      map.set(c.id, match.id)
    }
  }
  return map
}

/**
 * 是否可以播种：弹窗打开、代理池已返回**且不在刷新中**、且尚未播种过。
 *
 * `fetching` 不能省：二次打开时 query 缓存会立刻给出上次的代理池，照此播种会让
 * 「上次打开之后才被禁用的代理」在基线里仍是数字 id，而下拉只列可选代理，
 * 该行渲染成空白。等 refetch 落地一拍再播种。
 */
export function canSeed(
  open: boolean,
  proxyPool: { proxies: ProxyPoolEntry[] } | undefined,
  seeded: boolean,
  fetching: boolean,
): boolean {
  return open && proxyPool !== undefined && !fetching && !seeded
}

/** 与基线同帧同源产生的初始选值（只读行不进 selection）。 */
export function seedSelection(baseline: Baseline): Record<number, number | null> {
  const selection: Record<number, number | null> = {}
  for (const [id, v] of baseline) {
    if (isEditable(v)) selection[id] = v
  }
  return selection
}

/**
 * 提交体：只含被用户改动过的行。
 *
 * 以 `baseline` 为遍历源而非实时凭据列表——播种后新出现的凭据不在基线里，
 * 既不参与 diff 也不会被提交（用户从未看见过它们的初值，不能替他决定）。
 */
export function diffAssignments(
  baseline: Baseline,
  selection: Record<number, number | null>,
): BatchAssignEntry[] {
  const out: BatchAssignEntry[] = []
  for (const [credentialId, initial] of baseline) {
    if (!isEditable(initial)) continue
    const current = selection[credentialId] ?? null
    if (current !== initial) out.push({ credentialId, proxyId: current })
  }
  return out
}
