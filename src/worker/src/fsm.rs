#[derive(Debug, Default)]
pub(super) struct Fsm;

#[derive(Debug)]
pub(super) enum Command {}

#[derive(Debug, Default)]
pub(super) struct Effects;

impl Fsm {
    pub(super) fn on_apply(&self, _command: Command) -> Effects {
        Effects::default()
    }
}
