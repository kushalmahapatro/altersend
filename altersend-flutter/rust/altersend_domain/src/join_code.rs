use regex::Regex;
use std::sync::LazyLock;

pub const JOIN_URL_SCHEME: &str = "com.altersend.mobile";

static JOIN_CODE_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-fA-F0-9]{64}$").expect("valid join code regex"));

static EXTRACT_PATTERN: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"[a-fA-F0-9]{64}").expect("valid extract regex"));

pub fn build_join_url(topic: &str) -> String {
    format!("{JOIN_URL_SCHEME}://join/{topic}")
}

pub fn is_valid_join_code(value: &str) -> bool {
    JOIN_CODE_PATTERN.is_match(value.trim())
}

pub fn extract_join_code(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    EXTRACT_PATTERN
        .find(trimmed)
        .map(|m| m.as_str().to_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_code() {
        let code = "a".repeat(64);
        assert!(is_valid_join_code(&code));
    }

    #[test]
    fn extracts_from_url() {
        let code = "b".repeat(64);
        let url = format!("{JOIN_URL_SCHEME}://join/{code}");
        assert_eq!(extract_join_code(&url).as_deref(), Some(code.as_str()));
    }
}
