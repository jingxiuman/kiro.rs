//! Claude Code request metadata helpers.

use uuid::Uuid;

/// Extract the Claude Code session UUID from `metadata.user_id`.
///
/// Claude Code 2.1.78 and newer use a JSON object encoded as a string. Older
/// versions use `user_<hash>_account_<uuid>_session_<uuid>`.
pub(crate) fn extract_session_id(user_id: &str) -> Option<String> {
    let user_id = user_id.trim();
    if user_id.is_empty() {
        return None;
    }

    if user_id.starts_with('{') {
        let value = serde_json::from_str::<serde_json::Value>(user_id).ok()?;
        return value
            .get("session_id")
            .and_then(serde_json::Value::as_str)
            .and_then(valid_session_id);
    }

    let (_, suffix) = user_id.split_once("_session_")?;
    let candidate = suffix.get(..36)?;
    valid_session_id(candidate)
}

fn valid_session_id(candidate: &str) -> Option<String> {
    Uuid::parse_str(candidate)
        .ok()
        .map(|session_id| session_id.hyphenated().to_string())
}

/// OpenAI 协议侧会话标识的 `metadata.user_id` 前缀。
///
/// 不用上游的裸 `session_<uuid>`：本文件的解析走 `split_once("_session_")`，
/// 裸形式前面没有下划线会解析失败。带前缀既能被既有解析器识别，又给缓存计量
/// 的隔离种子提供了「这条 session 来自客户端可控字段」的判据
/// （见 `cache_metering::isolation_seed`）。
pub(crate) const OPENAI_SESSION_USER_ID_PREFIX: &str = "openai_client__session_";

/// 把 OpenAI 侧解析出的会话 UUID 包装成 `metadata.user_id`。
pub(crate) fn openai_session_user_id(session: &Uuid) -> String {
    format!("{OPENAI_SESSION_USER_ID_PREFIX}{}", session.hyphenated())
}

/// 该 `user_id` 是否由 OpenAI 协议入口构造。
pub(crate) fn is_openai_client_session(user_id: &str) -> bool {
    user_id.trim().starts_with(OPENAI_SESSION_USER_ID_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::extract_session_id;

    const SESSION_ID: &str = "8bb5523b-ec7c-4540-a9ca-beb6d79f1552";

    #[test]
    fn parses_json_metadata_used_by_current_claude_code() {
        let user_id =
            format!(r#"{{"device_id":"device","account_uuid":"","session_id":"{SESSION_ID}"}}"#);
        assert_eq!(extract_session_id(&user_id).as_deref(), Some(SESSION_ID));
    }

    #[test]
    fn parses_legacy_metadata() {
        let user_id = format!("user_hash_account__session_{SESSION_ID}");
        assert_eq!(extract_session_id(&user_id).as_deref(), Some(SESSION_ID));
    }

    #[test]
    fn rejects_missing_or_invalid_session_ids() {
        assert_eq!(extract_session_id(""), None);
        assert_eq!(extract_session_id(r#"{"device_id":"device"}"#), None);
        assert_eq!(
            extract_session_id(r#"{"device_id":"device","session_id":"not-a-uuid"}"#),
            None
        );
        assert_eq!(extract_session_id("user_hash_account__session_short"), None);
    }

    #[test]
    fn openai_session_user_id_roundtrips_through_extract() {
        let uuid = uuid::Uuid::parse_str(SESSION_ID).unwrap();
        let user_id = super::openai_session_user_id(&uuid);
        assert_eq!(user_id, format!("openai_client__session_{SESSION_ID}"));
        // 必须能被既有解析器识别，否则 conversationId 推导拿不到它
        assert_eq!(extract_session_id(&user_id), Some(SESSION_ID.to_string()));
        assert!(super::is_openai_client_session(&user_id));
        // Claude Code 的两种形态都不得被误判为 OpenAI 来源
        assert!(!super::is_openai_client_session(&format!(
            "user_xxx_account__session_{SESSION_ID}"
        )));
        assert!(!super::is_openai_client_session(&format!(
            r#"{{"session_id":"{SESSION_ID}"}}"#
        )));
    }
}
