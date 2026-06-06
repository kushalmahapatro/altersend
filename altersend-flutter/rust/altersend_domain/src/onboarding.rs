use serde::{Deserialize, Serialize};

pub const PRIVACY_POLICY_URL: &str = "https://altersend.com/privacy";
pub const TERMS_OF_SERVICE_URL: &str = "https://altersend.com/terms";
pub const ABUSE_EMAIL: &str = "abuse@altersend.com";
pub const SUPPORT_EMAIL: &str = "hello@altersend.com";
pub const WEBSITE_URL: &str = "https://altersend.com";
pub const GITHUB_URL: &str = "https://github.com/denislupookov/altersend";
pub const DISCORD_URL: &str = "https://discord.gg/R6tmrk85Vx";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OnboardingSlideKind {
    Pairing,
    KeepOpen,
    Privacy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnboardingSlideLink {
    pub label: String,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnboardingSlide {
    pub kind: OnboardingSlideKind,
    pub title: String,
    pub subtitle: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link: Option<OnboardingSlideLink>,
}

pub fn onboarding_slides() -> Vec<OnboardingSlide> {
    vec![
        OnboardingSlide {
            kind: OnboardingSlideKind::Pairing,
            title: "Files, directly between devices.".to_string(),
            subtitle: "One device sends, the other receives. A short code connects them so files can stream directly.".to_string(),
            link: None,
        },
        OnboardingSlide {
            kind: OnboardingSlideKind::KeepOpen,
            title: "Keep both apps open.".to_string(),
            subtitle: "Files stream directly between devices — there is no cloud. Closing or backgrounding the app will cancel the transfer.".to_string(),
            link: None,
        },
        OnboardingSlide {
            kind: OnboardingSlideKind::Privacy,
            title: "End-to-end encrypted.".to_string(),
            subtitle: "No servers, no copies, no middlemen. Your files travel peer-to-peer between you and the recipient.".to_string(),
            link: Some(OnboardingSlideLink {
                label: "Read our privacy policy".to_string(),
                url: PRIVACY_POLICY_URL.to_string(),
            }),
        },
    ]
}

pub const SENDER_KEEP_OPEN_HINT: &str = "Closing the app cancels the transfer.";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn has_three_slides() {
        assert_eq!(onboarding_slides().len(), 3);
    }

    #[test]
    fn privacy_slide_has_link() {
        let slides = onboarding_slides();
        let privacy = slides.iter().find(|s| s.kind == OnboardingSlideKind::Privacy);
        assert!(privacy.unwrap().link.is_some());
    }
}
