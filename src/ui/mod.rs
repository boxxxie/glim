use chrono::{DateTime, Duration, Local};
use compact_str::{format_compact, CompactString, ToCompactString};
use ratatui::text::Span;

use crate::theme::theme;

pub mod fx;
pub mod popup;
mod stateful_widgets;
pub mod widget;

pub use stateful_widgets::{ClickAction, ClickRegion, StatefulWidgets};

pub fn format_duration(duration: Duration) -> CompactString {
    let total_seconds = duration.abs().num_seconds();
    let hours = total_seconds / 3600;
    let minutes = (total_seconds % 3600) / 60;
    let seconds = total_seconds % 60;

    match () {
        _ if hours > 0 => format_compact!("{}:{:02}:{:02}", hours, minutes, seconds),
        _ if minutes > 0 => format_compact!("{}:{:02}", minutes, seconds),
        _ => format_compact!("0:{:02}", seconds),
    }
}

/// If `date` is within the past 24 hours of `now`, returns a compact relative string
/// such as `"just now"`, `"30s ago"`, `"1m ago"`, `"45m ago"`, `"1h ago"`, `"1.5h ago"`.
/// Returns `None` if `date` is 24 hours or older.
pub fn format_relative_24h(date: DateTime<Local>, now: DateTime<Local>) -> Option<CompactString> {
    let secs = now.signed_duration_since(date).num_seconds();
    if secs < 0 {
        return Some("just now".into());
    }
    if secs >= 86_400 {
        return None;
    }
    if secs < 10 {
        return Some("just now".into());
    }
    if secs < 60 {
        return Some(format_compact!("{}s ago", secs));
    }
    let mins = secs / 60;
    if mins < 60 {
        return Some(format_compact!("{}m ago", mins));
    }
    let half_hours = ((secs as f64) / 1800.0).round() as u32;
    let half_hours = half_hours.clamp(2, 47);
    if half_hours % 2 == 0 {
        Some(format_compact!("{}h ago", half_hours / 2))
    } else {
        Some(format_compact!("{}.5h ago", half_hours / 2))
    }
}

/// Formats a timestamp into fixed-width (12-char) spans using relative time (`"1m ago"`, `"1.5h ago"`)
/// for the past 24 hours and `"20 Oct 14:12"` for older timestamps.
pub fn format_timestamp_spans(date: DateTime<Local>) -> Vec<Span<'static>> {
    if let Some(rel) = format_relative_24h(date, Local::now()) {
        vec![Span::from(format_compact!("{:<12}", rel)).style(theme().time)]
    } else {
        vec![
            Span::from(date.format("%d %b ").to_compact_string()).style(theme().date),
            Span::from(date.format("%H:%M").to_compact_string()).style(theme().time),
        ]
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeDelta;

    use super::*;

    #[test]
    fn test_format_relative_24h() {
        let now = Local::now();
        assert_eq!(
            format_relative_24h(now - TimeDelta::seconds(3), now).as_deref(),
            Some("just now")
        );
        assert_eq!(
            format_relative_24h(now - TimeDelta::seconds(42), now).as_deref(),
            Some("42s ago")
        );
        assert_eq!(
            format_relative_24h(now - TimeDelta::minutes(1), now).as_deref(),
            Some("1m ago")
        );
        assert_eq!(
            format_relative_24h(now - TimeDelta::minutes(45), now).as_deref(),
            Some("45m ago")
        );
        assert_eq!(
            format_relative_24h(now - TimeDelta::minutes(62), now).as_deref(),
            Some("1h ago")
        );
        assert_eq!(
            format_relative_24h(now - TimeDelta::minutes(90), now).as_deref(),
            Some("1.5h ago")
        );
        assert_eq!(
            format_relative_24h(now - TimeDelta::hours(24), now),
            None
        );
    }
}
