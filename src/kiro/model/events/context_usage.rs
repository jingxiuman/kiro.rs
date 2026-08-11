//! 上下文使用率事件
//!
//! 处理 contextUsageEvent 类型的事件

use serde::Deserialize;

use crate::kiro::parser::error::ParseResult;
use crate::kiro::parser::frame::Frame;

use super::base::EventPayload;

/// 上下文使用率事件
///
/// 包含当前上下文窗口的使用百分比
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextUsageEvent {
    /// 上下文使用百分比 (0-100)
    #[serde(default)]
    pub context_usage_percentage: f64,
}

impl EventPayload for ContextUsageEvent {
    fn from_frame(frame: &Frame) -> ParseResult<Self> {
        frame.payload_as_json()
    }
}

/// 百分比→token 反算的安全系数。
///
/// 客户端（Claude Code 等）依据 usage 水位决定何时自动压缩历史，观测到的触发点
/// 约在窗口的 96%。上游墙（CONTENT_LENGTH_EXCEEDS_THRESHOLD）实测恰为窗口值本身
/// （2026-08-11 生产二分探针：[997,714 过, ~1,005,000 挂]，窗口 1M）。若按真值上报，
/// 客户端要到 ~96% 真实水位才压缩，而压缩请求自身携带全量历史，稍有增长即越墙，
/// 落入「继续能跑、压缩必死」的死区（2026-08-10 session f2f77a4b 实例）。
/// 乘 1.08 后客户端在 ~89% 真实水位即触发压缩，留出 >100k token 的压缩余量。
/// 代价：正常会话提前 ~7% 压缩，可接受。
const CONTEXT_USAGE_SAFETY_FACTOR: f64 = 1.08;

impl ContextUsageEvent {
    /// 获取格式化的百分比字符串
    pub fn formatted_percentage(&self) -> String {
        format!("{:.2}%", self.context_usage_percentage)
    }

    /// 按窗口大小反算 input_tokens，并施加安全系数（上限钳到窗口值）。
    ///
    /// 所有把 contextUsageEvent 换算成 usage token 的路径（流式 / 非流式 /
    /// web-search）必须走这里，保证同一口径。钳位到窗口值是为了不向客户端
    /// 报出超过其认知窗口的数字（百分比本身也在 100% 处钳位，语义一致）。
    pub fn input_tokens_with_margin(&self, context_window: i32) -> i32 {
        let raw = self.context_usage_percentage * (context_window as f64) / 100.0
            * CONTEXT_USAGE_SAFETY_FACTOR;
        (raw as i32).min(context_window)
    }

    /// 上下文是否已顶满（≥100%，会话已进入「压缩必死」死区的信号）。
    pub fn is_exhausted(&self) -> bool {
        self.context_usage_percentage >= 100.0
    }
}

impl std::fmt::Display for ContextUsageEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.formatted_percentage())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(pct: f64) -> ContextUsageEvent {
        ContextUsageEvent {
            context_usage_percentage: pct,
        }
    }

    /// 中低水位：反算 = pct × 窗口 × 1.08。50% × 1M → 540k，
    /// 客户端看到的水位始终略高于真值，压缩提前触发。
    #[test]
    fn margin_applied_below_cap() {
        assert_eq!(ev(50.0).input_tokens_with_margin(1_000_000), 540_000);
        assert_eq!(ev(0.0).input_tokens_with_margin(1_000_000), 0);
    }

    /// 高水位钳到窗口值：≥ 100/1.08 ≈ 92.6% 后统一报窗口值，
    /// 不向客户端报出超过其认知窗口的数字（百分比本身在 100% 钳位，语义一致）。
    #[test]
    fn margin_capped_at_window() {
        assert_eq!(ev(96.0).input_tokens_with_margin(1_000_000), 1_000_000);
        assert_eq!(ev(100.0).input_tokens_with_margin(1_000_000), 1_000_000);
    }

    #[test]
    fn exhausted_at_100() {
        assert!(ev(100.0).is_exhausted());
        assert!(!ev(99.9).is_exhausted());
    }
}
