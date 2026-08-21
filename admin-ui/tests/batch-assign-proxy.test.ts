// 用 bun 内置 test runner 跑（无新增依赖）：
//   cd admin-ui && /root/.bun/bin/bun test
//
// 放在 src 之外：tsconfig 的 include 只有 src，避免 `tsc -b` 去找 bun:test 的类型。
import { describe, expect, test } from 'bun:test'
import {
  buildBaseline,
  canSeed,
  diffAssignments,
  isEditable,
  seedSelection,
} from '../src/lib/batch-assign-proxy'
import type { CredentialStatusItem, ProxyPoolEntry } from '../src/types/api'

function proxy(id: number, url: string, over: Partial<ProxyPoolEntry> = {}): ProxyPoolEntry {
  return {
    id,
    url,
    enabled: true,
    credentialCount: 0,
    health: 'healthy',
    consecutiveFailures: 0,
    autoDisabled: false,
    requestFailures: 0,
    ...over,
  } as ProxyPoolEntry
}

function cred(id: number, proxyUrl?: string): CredentialStatusItem {
  return { id, proxyUrl } as CredentialStatusItem
}

const POOL = [proxy(11, 'http://127.0.0.1:1080'), proxy(12, 'http://127.0.0.1:1081')]
const CREDS = Array.from({ length: 8 }, (_, i) =>
  cred(i + 1, `http://127.0.0.1:${1080 + (i % 2)}`),
)

describe('批量绑代理弹窗的基线', () => {
  test('代理池未返回前不得播种（C1 竞态的根因）', () => {
    expect(canSeed(true, undefined, false)).toBe(false)
    expect(canSeed(true, { proxies: POOL }, false)).toBe(true)
    // 已播种后不重播：弹窗开着时的 30s 轮询不得改动基线
    expect(canSeed(true, { proxies: POOL }, true)).toBe(false)
    expect(canSeed(false, { proxies: POOL }, false)).toBe(false)
  })

  test('播种后不动任何一行，提交体为空（首开必现的批量误解绑）', () => {
    const baseline = buildBaseline(CREDS, POOL)
    const selection = seedSelection(baseline)
    expect(diffAssignments(baseline, selection)).toEqual([])
  })

  test('播种后凭据列表刷新，不进基线也不进提交体', () => {
    const baseline = buildBaseline(CREDS, POOL)
    const selection = seedSelection(baseline)
    // 外部新增一张凭据 + 外部把 #1 改绑到另一个代理，基线是冻结的
    const refreshed = [...CREDS.map((c) => (c.id === 1 ? cred(1, POOL[1].url) : c)), cred(99)]
    expect(refreshed.length).toBe(9)
    expect(diffAssignments(baseline, selection)).toEqual([])
    // 新凭据不在基线里 → 不渲染、不提交
    expect(baseline.has(99)).toBe(false)
  })

  test('只提交用户真正改动的行', () => {
    const baseline = buildBaseline(CREDS, POOL)
    const selection = { ...seedSelection(baseline), 1: 12, 2: null }
    expect(diffAssignments(baseline, selection)).toEqual([
      { credentialId: 1, proxyId: 12 },
      { credentialId: 2, proxyId: null },
    ])
  })

  test('自定义 URL 与已禁用代理都按只读处理，不进提交体（M6）', () => {
    const pool = [
      proxy(11, 'http://127.0.0.1:1080'),
      proxy(12, 'http://127.0.0.1:1081', { enabled: false }),
      proxy(13, 'http://127.0.0.1:1082', { autoDisabled: true }),
    ]
    const baseline = buildBaseline(
      [cred(1, pool[0].url), cred(2, pool[1].url), cred(3, pool[2].url), cred(4, 'socks5://x'), cred(5)],
      pool,
    )
    expect(baseline.get(1)).toBe(11)
    expect(baseline.get(2)).toEqual({ kind: 'disabled', url: 'http://127.0.0.1:1081' })
    expect(baseline.get(3)).toEqual({ kind: 'disabled', url: 'http://127.0.0.1:1082' })
    expect(baseline.get(4)).toEqual({ kind: 'custom', url: 'socks5://x' })
    expect(baseline.get(5)).toBe(null)

    const selection = seedSelection(baseline)
    // 只读行不进 selection，也不会因为「selection 里没有 → 当成 null」而被误提交
    expect(Object.keys(selection).sort()).toEqual(['1', '5'])
    expect(diffAssignments(baseline, selection)).toEqual([])
    expect(isEditable(baseline.get(2))).toBe(false)
    expect(isEditable(baseline.get(5))).toBe(true)
  })
})
