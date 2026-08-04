//! 推理内容事件
//!
//! 处理 reasoningContentEvent 类型的事件。

use serde::Deserialize;

use crate::kiro::parser::error::ParseResult;
use crate::kiro::parser::frame::Frame;

use super::base::EventPayload;

/// Kiro 原生 thinking / reasoning 事件。
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReasoningContentEvent {
    /// 明文思考内容片段。
    #[serde(default)]
    pub text: Option<String>,
    /// 思考块签名。保留字段以完整反映上游线格式，但**有意不使用**：
    /// Bedrock 真签名可达 208KB（为正文 5-18 倍），下发只会膨胀客户端历史，
    /// 且回传时 ContentBlock 无此字段、serde 静默丢弃，从不回到上游。
    /// 下发统一用占位符（见 stream.rs THINKING_SIGNATURE_PLACEHOLDER）。
    #[serde(default)]
    #[allow(dead_code)]
    pub signature: Option<String>,
    /// 上游返回的加密思考内容。
    #[serde(default)]
    pub redacted_content: Option<String>,
}

impl EventPayload for ReasoningContentEvent {
    fn from_frame(frame: &Frame) -> ParseResult<Self> {
        frame.payload_as_json()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_text_signature_payload() {
        let v: ReasoningContentEvent =
            serde_json::from_str(r#"{"text":"abc","signature":"sig"}"#).unwrap();
        assert_eq!(v.text.as_deref(), Some("abc"));
        assert_eq!(v.signature.as_deref(), Some("sig"));
    }

    #[test]
    fn parse_redacted_payload() {
        let v: ReasoningContentEvent =
            serde_json::from_str(r#"{"redactedContent":"encrypted"}"#).unwrap();
        assert_eq!(v.redacted_content.as_deref(), Some("encrypted"));
    }
}
