//! 事件基础定义
//!
//! 定义事件类型枚举、trait 和统一事件结构

use crate::kiro::parser::error::{ParseError, ParseResult};
use crate::kiro::parser::frame::Frame;

/// 事件类型枚举
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EventType {
    /// 助手响应事件
    AssistantResponse,
    /// 工具使用事件
    ToolUse,
    /// 计费事件
    Metering,
    /// 上下文使用率事件
    ContextUsage,
    /// 推理内容事件
    ReasoningContent,
    /// 未知事件类型
    Unknown,
}

impl EventType {
    /// 从事件类型字符串解析
    pub fn from_str(s: &str) -> Self {
        match s {
            "assistantResponseEvent" => Self::AssistantResponse,
            "toolUseEvent" => Self::ToolUse,
            "meteringEvent" => Self::Metering,
            "contextUsageEvent" => Self::ContextUsage,
            "reasoningContentEvent" => Self::ReasoningContent,
            _ => Self::Unknown,
        }
    }

    /// 转换为事件类型字符串
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AssistantResponse => "assistantResponseEvent",
            Self::ToolUse => "toolUseEvent",
            Self::Metering => "meteringEvent",
            Self::ContextUsage => "contextUsageEvent",
            Self::ReasoningContent => "reasoningContentEvent",
            Self::Unknown => "unknown",
        }
    }
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// 事件 payload trait
///
/// 所有具体事件类型都需要实现此 trait
pub trait EventPayload: Sized {
    /// 从帧解析事件负载
    fn from_frame(frame: &Frame) -> ParseResult<Self>;
}

/// 统一事件枚举
///
/// 封装所有可能的事件类型
#[derive(Debug, Clone)]
pub enum Event {
    /// 助手响应
    AssistantResponse(super::AssistantResponseEvent),
    /// 工具使用
    ToolUse(super::ToolUseEvent),
    /// 计费
    Metering(super::MeteringEvent),
    /// 上下文使用率
    ContextUsage(super::ContextUsageEvent),
    /// 推理内容
    ReasoningContent(super::ReasoningContentEvent),
    /// 未知事件 (保留原始帧数据)
    Unknown {},
    /// 服务端错误
    Error {
        /// 错误代码
        error_code: String,
        /// 错误消息
        error_message: String,
    },
    /// 服务端异常
    Exception {
        /// 异常类型
        exception_type: String,
        /// 异常消息
        message: String,
    },
}

/// 已上报过的未知事件名。只在 Unknown 分支访问，不在热路径上。
static SEEN_UNKNOWN_EVENTS: std::sync::OnceLock<parking_lot::Mutex<std::collections::HashSet<String>>> =
    std::sync::OnceLock::new();

/// 登记一个未知事件名，返回 `true` 表示本进程内首次见到（调用方据此只 warn 一次）。
fn note_unknown_event(name: &str) -> bool {
    SEEN_UNKNOWN_EVENTS
        .get_or_init(Default::default)
        .lock()
        .insert(name.to_string())
}

#[cfg(test)]
fn reset_seen_unknown_events_for_test() {
    SEEN_UNKNOWN_EVENTS.get_or_init(Default::default).lock().clear();
}

impl Event {
    /// 从帧解析事件
    pub fn from_frame(frame: Frame) -> ParseResult<Self> {
        let message_type = frame.message_type().unwrap_or("event");

        match message_type {
            "event" => Self::parse_event(frame),
            "error" => Self::parse_error(frame),
            "exception" => Self::parse_exception(frame),
            other => Err(ParseError::InvalidMessageType(other.to_string())),
        }
    }

    /// 解析事件类型消息
    fn parse_event(frame: Frame) -> ParseResult<Self> {
        let event_type_str = frame.event_type().unwrap_or("unknown");
        let event_type = EventType::from_str(event_type_str);

        match event_type {
            EventType::AssistantResponse => {
                let payload = super::AssistantResponseEvent::from_frame(&frame)?;
                Ok(Self::AssistantResponse(payload))
            }
            EventType::ToolUse => {
                let payload = super::ToolUseEvent::from_frame(&frame)?;
                Ok(Self::ToolUse(payload))
            }
            EventType::Metering => {
                let payload = super::MeteringEvent::from_frame(&frame)?;
                Ok(Self::Metering(payload))
            }
            EventType::ContextUsage => {
                let payload = super::ContextUsageEvent::from_frame(&frame)?;
                Ok(Self::ContextUsage(payload))
            }
            EventType::ReasoningContent => {
                let payload = super::ReasoningContentEvent::from_frame(&frame)?;
                Ok(Self::ReasoningContent(payload))
            }
            EventType::Unknown => {
                // 上游新增事件不会自己冒头——这里是唯一能看见它的地方。
                // 按事件名去重，避免流式高频刷屏。
                if let Some(name) = frame.event_type()
                    && note_unknown_event(name)
                {
                    tracing::warn!(event_type = %name, "上游返回了未识别的事件类型（本进程首次）");
                }
                Ok(Self::Unknown {})
            }
        }
    }

    /// 解析错误类型消息
    fn parse_error(frame: Frame) -> ParseResult<Self> {
        let error_code = frame
            .headers
            .error_code()
            .unwrap_or("UnknownError")
            .to_string();
        let error_message = frame.payload_as_str();

        Ok(Self::Error {
            error_code,
            error_message,
        })
    }

    /// 解析异常类型消息
    fn parse_exception(frame: Frame) -> ParseResult<Self> {
        let exception_type = frame
            .headers
            .exception_type()
            .unwrap_or("UnknownException")
            .to_string();
        let message = frame.payload_as_str();

        Ok(Self::Exception {
            exception_type,
            message,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_type_from_str() {
        assert_eq!(
            EventType::from_str("assistantResponseEvent"),
            EventType::AssistantResponse
        );
        assert_eq!(EventType::from_str("toolUseEvent"), EventType::ToolUse);
        assert_eq!(EventType::from_str("meteringEvent"), EventType::Metering);
        assert_eq!(
            EventType::from_str("contextUsageEvent"),
            EventType::ContextUsage
        );
        assert_eq!(
            EventType::from_str("reasoningContentEvent"),
            EventType::ReasoningContent
        );
        assert_eq!(EventType::from_str("unknown_type"), EventType::Unknown);
    }

    #[test]
    fn test_event_type_as_str() {
        assert_eq!(
            EventType::AssistantResponse.as_str(),
            "assistantResponseEvent"
        );
        assert_eq!(EventType::ToolUse.as_str(), "toolUseEvent");
    }

    /// 未知事件仍解析为 Unknown（形状不变），但事件名会被记录一次。
    #[test]
    fn unknown_event_name_is_recorded_once() {
        super::reset_seen_unknown_events_for_test();
        assert!(super::note_unknown_event("metadataEvent"), "首次出现必须上报");
        assert!(!super::note_unknown_event("metadataEvent"), "同名不得重复上报");
        assert!(super::note_unknown_event("someOtherEvent"), "新名字仍要上报");
    }

    /// 构造一个 event-type 头为给定值、message-type 为 "event" 的 Frame，
    /// 供 `from_frame` 相关测试复用。
    fn frame_with_event_type(event_type: &str) -> Frame {
        let mut headers = crate::kiro::parser::header::Headers::new();
        headers.insert(
            ":message-type".to_string(),
            crate::kiro::parser::header::HeaderValue::String("event".to_string()),
        );
        headers.insert(
            ":event-type".to_string(),
            crate::kiro::parser::header::HeaderValue::String(event_type.to_string()),
        );
        Frame {
            headers,
            payload: b"{}".to_vec(),
        }
    }

    /// 验证 `Event::from_frame` 在 Unknown 分支登记的是「原始事件名」而非
    /// `EventType::Unknown.as_str()`（即字符串 "unknown"）。
    ///
    /// 取证价值：这个改动的唯一目的是让日志里出现真实的上游事件名，
    /// 如果登记的其实是 "unknown"，取证就失效了。
    ///
    /// 前提：`SEEN_UNKNOWN_EVENTS` 是进程级全局状态，与
    /// `unknown_event_name_is_recorded_once` 共享。为避免互相污染，本测试
    /// 使用与该测试不同的事件名（"metadataEvent" / "someOtherEvent" 已被占用），
    /// 不做全局 reset（reset 会与并行跑的其他测试产生竞态）。
    #[test]
    fn from_frame_unknown_branch_records_the_real_event_name() {
        let frame = frame_with_event_type("weirdUpstreamEvent");

        let event = Event::from_frame(frame).expect("Unknown 分支不应返回错误");
        assert!(matches!(event, Event::Unknown {}), "未知事件应解析为 Event::Unknown {{}}");

        // 已被 from_frame 登记过，再次登记应返回 false。
        assert!(
            !super::note_unknown_event("weirdUpstreamEvent"),
            "from_frame 应已登记原始事件名 weirdUpstreamEvent，而不是字符串 \"unknown\""
        );
        // 一个从未出现过的名字应仍返回 true，证明登记的确是按真实事件名区分的。
        assert!(
            super::note_unknown_event("neverSeenBeforeEvent"),
            "全新事件名应首次上报"
        );
    }
}
