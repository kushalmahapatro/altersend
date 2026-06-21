use crate::types::{TransferRole, TransferSessionState};

/// Whether an incoming deep-link join code may be accepted in the current session.
pub fn can_join_from_deep_link(state: &TransferSessionState, code: &str) -> bool {
    if state.role == Some(TransferRole::Sender) {
        return false;
    }
    if !state.topic.is_empty() && state.topic != code {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::TransferSessionState;

    #[test]
    fn rejects_when_sender() {
        let state = TransferSessionState {
            role: Some(TransferRole::Sender),
            ..Default::default()
        };
        assert!(!can_join_from_deep_link(&state, &"a".repeat(64)));
    }

    #[test]
    fn rejects_conflicting_topic() {
        let state = TransferSessionState {
            topic: "b".repeat(64),
            role: Some(TransferRole::Receiver),
            ..Default::default()
        };
        assert!(!can_join_from_deep_link(&state, &"a".repeat(64)));
    }

    #[test]
    fn accepts_idle() {
        assert!(can_join_from_deep_link(&TransferSessionState::default(), &"a".repeat(64)));
    }
}
