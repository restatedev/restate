use tracing::debug;

#[derive(Debug, Default)]
pub(super) struct Fsm;

#[derive(Debug)]
pub(super) enum Command {}

#[derive(Debug, Default)]
pub(super) struct Effects;

impl Fsm {
    pub(super) fn on_apply(&self, command: Command) -> Effects {
        debug!(?command, "Apply");
        Effects::default()
    }
}
