use std::sync::mpsc::Sender;

use crossterm::event::{KeyCode, KeyEvent};

use crate::{dispatcher::Dispatcher, event::GlimEvent, input::InputProcessor, ui::StatefulWidgets};

pub struct ActivityDetailsProcessor {
    sender: Sender<GlimEvent>,
}

impl ActivityDetailsProcessor {
    pub fn new(sender: Sender<GlimEvent>) -> Self {
        Self { sender }
    }

    fn process(&self, event: &KeyEvent, ui: &mut StatefulWidgets) {
        match event.code {
            KeyCode::Esc | KeyCode::Char('q') => {
                self.sender.dispatch(GlimEvent::ActivityDetailsClose);
            },
            KeyCode::Up | KeyCode::Char('k') => ui.handle_activity_action_selection(-1),
            KeyCode::Down | KeyCode::Char('j') => ui.handle_activity_action_selection(1),
            KeyCode::Enter | KeyCode::Char('o') => {
                let action = ui
                    .activity_details
                    .as_ref()
                    .and_then(|s| s.selected_action_event());
                self.sender.dispatch(GlimEvent::ActivityDetailsClose);
                if let Some(action) = action {
                    self.sender.dispatch(action);
                }
            },
            KeyCode::Char('w') => {
                if let Some(state) = ui.activity_details.as_ref() {
                    let ev = &state.event;
                    let open_ev = if let Some(iid) = ev.mr_iid {
                        GlimEvent::MergeRequestOpenUrl(ev.project_id, iid)
                    } else {
                        GlimEvent::ProjectOpenUrl(ev.project_id)
                    };
                    self.sender.dispatch(GlimEvent::ActivityDetailsClose);
                    self.sender.dispatch(open_ev);
                }
            },
            KeyCode::F(12) => self.sender.dispatch(GlimEvent::ScreenCapture),
            _ => (),
        }
    }
}

impl InputProcessor for ActivityDetailsProcessor {
    fn apply(&mut self, event: &GlimEvent, ui: &mut StatefulWidgets) {
        if let GlimEvent::InputKey(e) = event {
            self.process(e, ui)
        }
    }

    fn on_pop(&self) {}
    fn on_push(&self) {}
}
