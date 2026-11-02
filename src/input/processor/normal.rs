use std::sync::mpsc::Sender;

use compact_str::ToCompactString;
use crossterm::event::{KeyCode, KeyEvent};

use crate::{
    dispatcher::Dispatcher, domain::RowEntryTarget, event::GlimEvent, id::ProjectId,
    input::InputProcessor, ui::StatefulWidgets,
};

pub struct NormalModeProcessor {
    sender: Sender<GlimEvent>,
    selected: Option<ProjectId>,
}

impl NormalModeProcessor {
    pub fn new(sender: Sender<GlimEvent>) -> Self {
        Self { sender, selected: None }
    }

    fn process(&self, event: &KeyEvent, ui: &StatefulWidgets) {
        let feed_focused = ui.activity_feed_focused && ui.show_activity_feed;
        let flat_active = !ui.current_flat_targets.is_empty();
        if let Some(e) = match event.code {
            KeyCode::Tab | KeyCode::BackTab => Some(GlimEvent::FocusToggle),
            KeyCode::Char('v') => Some(GlimEvent::ToggleCiViewMode),
            KeyCode::Char('s') => Some(GlimEvent::CycleRankingMode),
            KeyCode::Char('S') => Some(GlimEvent::RerollStochastic),
            KeyCode::Enter | KeyCode::Char('o') if feed_focused => {
                ui.selected_activity_id.map(GlimEvent::ActivityDetailsOpen)
            },
            KeyCode::Enter | KeyCode::Char('o') if flat_active => {
                if let Some((proj_id, _, Some(pid))) = ui.selected_flat_target {
                    Some(GlimEvent::ProjectDetailsOpenWithPipeline(proj_id, pid))
                } else {
                    self.selected.map(GlimEvent::ProjectDetailsOpen)
                }
            },
            KeyCode::Enter if self.selected.is_some() => {
                Some(GlimEvent::ProjectDetailsOpen(self.selected.unwrap()))
            },
            KeyCode::Char('o') if self.selected.is_some() => {
                Some(GlimEvent::ProjectDetailsOpen(self.selected.unwrap()))
            },
            KeyCode::Char('a') => Some(GlimEvent::NotificationLast),
            KeyCode::Char('c') => Some(GlimEvent::ConfigOpen),
            KeyCode::Char('f') => Some(GlimEvent::FilterMenuShow),
            KeyCode::Char('/') => Some(GlimEvent::FilterMenuShow),
            KeyCode::Char('m') => Some(GlimEvent::ToggleActivityFeed),
            KeyCode::Char('p') => {
                if let Some(proj_id) = self.selected {
                    self.dispatch(GlimEvent::ProjectManualRefresh(proj_id));
                    Some(GlimEvent::PipelinesFetch(proj_id))
                } else {
                    None
                }
            },
            KeyCode::Char('q') => Some(GlimEvent::AppExit),
            KeyCode::Char('r') => Some(GlimEvent::ProjectsFetch),
            KeyCode::Char('w') if flat_active => {
                if let Some((proj_id, ref target, _)) = ui.selected_flat_target {
                    Some(match target {
                        RowEntryTarget::MergeRequest(iid) => {
                            GlimEvent::MergeRequestOpenUrl(proj_id, *iid)
                        },
                        RowEntryTarget::Pipeline(pid) => GlimEvent::PipelineOpenUrl(proj_id, *pid),
                    })
                } else {
                    self.selected.map(GlimEvent::ProjectOpenUrl)
                }
            },
            KeyCode::Char('w') => self.selected.map(GlimEvent::ProjectOpenUrl),
            KeyCode::F(12) => Some(GlimEvent::ScreenCapture),
            KeyCode::Up | KeyCode::Char('k') if feed_focused => Some(GlimEvent::ActivityPrevious),
            KeyCode::Down | KeyCode::Char('j') if feed_focused => Some(GlimEvent::ActivityNext),
            KeyCode::PageUp if feed_focused => Some(GlimEvent::ActivityPageUp),
            KeyCode::PageDown if feed_focused => Some(GlimEvent::ActivityPageDown),
            KeyCode::Home if feed_focused => Some(GlimEvent::ActivityFirst),
            KeyCode::End if feed_focused => Some(GlimEvent::ActivityLast),
            KeyCode::Up | KeyCode::Char('k') => Some(GlimEvent::ProjectPrevious),
            KeyCode::Down | KeyCode::Char('j') => Some(GlimEvent::ProjectNext),
            KeyCode::PageUp => Some(GlimEvent::ProjectPageUp),
            KeyCode::PageDown => Some(GlimEvent::ProjectPageDown),
            KeyCode::Home => Some(GlimEvent::ProjectFirst),
            KeyCode::End => Some(GlimEvent::ProjectLast),
            KeyCode::Esc => Some(GlimEvent::FilterClear),
            _ => None,
        } {
            self.dispatch(e)
        }
    }

    fn process_filter_input(&self, event: &KeyEvent, _widgets: &mut StatefulWidgets) {
        match event.code {
            KeyCode::Enter => {
                // Filter is already applied, just close the input
                self.dispatch(GlimEvent::FilterMenuClose);
            },
            KeyCode::Esc => {
                // Cancel filter and reset to no filter
                self.dispatch(GlimEvent::ApplyTemporaryFilter(None));
                self.dispatch(GlimEvent::FilterMenuClose);
            },
            KeyCode::Backspace => {
                self.dispatch(GlimEvent::FilterInputBackspace);
            },
            KeyCode::Char(c) => {
                self.dispatch(GlimEvent::FilterInputChar(c.to_compact_string()));
            },
            _ => {},
        }
    }
}

impl InputProcessor for NormalModeProcessor {
    fn apply(&mut self, event: &GlimEvent, ui: &mut StatefulWidgets) {
        match event {
            GlimEvent::ProjectSelected(id) => self.selected = Some(*id),
            GlimEvent::InputKey(e) => {
                if ui.filter_input_active {
                    self.process_filter_input(e, ui);
                } else {
                    self.process(e, ui);
                }
            },
            _ => (),
        }
    }

    fn on_pop(&self) {}
    fn on_push(&self) {}
}

impl Dispatcher for NormalModeProcessor {
    fn dispatch(&self, event: GlimEvent) {
        self.sender.dispatch(event)
    }
}
