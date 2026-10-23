use std::process::Command;

use tracing::{debug, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum NotifyUrgency {
    Low,
    Normal,
    Critical,
}

impl NotifyUrgency {
    fn as_str(&self) -> &'static str {
        match self {
            NotifyUrgency::Low => "low",
            NotifyUrgency::Normal => "normal",
            NotifyUrgency::Critical => "critical",
        }
    }
}

/// Send a non-blocking desktop OS notification using `notify-send`.
pub fn send_os_notification(summary: &str, body: &str, urgency: NotifyUrgency) {
    debug!(
        summary = summary,
        body = body,
        urgency = urgency.as_str(),
        "Sending OS notification"
    );

    let result = Command::new("notify-send")
        .arg("-a")
        .arg("glim")
        .arg("-u")
        .arg(urgency.as_str())
        .arg(summary)
        .arg(body)
        .spawn();

    if let Err(e) = result {
        warn!(error = %e, "Failed to spawn notify-send for OS notification");
    }
}
