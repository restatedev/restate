use tracing::debug;

#[derive(Debug, Default)]
pub(super) struct StateMachine;

#[derive(Debug)]
pub(crate) enum Command {}

#[derive(Debug, Default)]
pub(super) struct Effects;

impl Effects {
    pub(super) fn clear(&mut self) {}
}

impl StateMachine {
    /// Applies the given command and returns effects via the provided effects struct
    ///
    /// We pass in the effects message as a mutable borrow to be able to reuse it across
    /// invocations of this methods which lies on the hot path.
    pub(super) fn on_apply(&self, command: Command, _effects: &mut Effects) {
        debug!(?command, "Apply");
    }
}
