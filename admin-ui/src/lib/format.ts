/**
 * 毫秒时长的紧凑展示：1s 以内显示 ms，否则显示两位小数的秒。
 *
 * 与 `ops-page.tsx` 里的同名局部函数不是同一实现——那份接受 `number | null | undefined`
 * 且只保留一位小数，用于概览页的聚合指标展示。两者行为不同，不要"顺手"合并成一份，
 * 否则会悄悄改变其中一个页面的渲染结果。此处只导出 trace-log-page 用的这一版
 * （`(ms: number)`，两位小数），`ops-page.tsx` 保留自己的局部副本。
 */
export function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  return `${(ms / 1000).toFixed(2)}s`
}
