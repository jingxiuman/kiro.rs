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
}
