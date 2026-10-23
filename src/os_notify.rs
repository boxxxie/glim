use std::process::{Command, Stdio};

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

    let summary_owned = summary.to_string();
    let body_owned = body.to_string();
    let urgency_str = urgency.as_str();

    std::thread::spawn(move || {
        let status = Command::new("notify-send")
            .arg("-a")
            .arg("glim")
            .arg("-u")
            .arg(urgency_str)
            .arg("-t")
            .arg("8000")
            .arg(&summary_owned)
            .arg(&body_owned)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();

        if let Err(e) = status {
            warn!(error = %e, "Failed to run notify-send for OS notification");
        }
    });
}
