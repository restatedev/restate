use restate_storage_api::idempotency_table::IdempotencyTable;
use restate_storage_api::invocation_status_table::InvocationStatusTable;
use restate_storage_api::journal_table::{JournalEntry, JournalTable};
use restate_types::identifiers::InvocationId;
use crate::Error;
use crate::partition::state_machine::ActionCollector;

// Desired outcome

struct GetStateJournalEntry {
    whatever_info: ()
}

struct AppendJournalEntry<JE> {
    invocation_id: InvocationId,
    other_stuff_from_invoker: (),
    journal_entry: JE
}

async fn handle<S>(
    entry: AppendJournalEntry<GetStateJournalEntry>,
    Storage(mut storage): Storage<S>,
    ActionCollector(mut collector) : ActionCollector
) -> Result<(), Error>
    where S: InvocationStatusTable + JournalTable{
    // Handle get state entry
}

// -------------- OR -----------------

impl<S> EventHandler<(Storage<S>, ActionCollector)> for AppendJournalEntry<GetStateJournalEntry> where S: InvocationStatusTable + JournalTable {
    async fn handle(self, (Storage(mut storage), ActionCollector(mut collector)): (Storage<S>, ActionCollector)) -> Result<(), Error> {
        // Handle get state entry
    }
}


/// --------- SCRATCHPAD

struct ActionCollector(pub Vec<()>);

struct Storage<S>(pub S);

trait EventHandler<CTX> {
    async fn handle(self, ctx: CTX) -> Result<(), Error>;
}


struct Context<'a, S> {
    storage: &'a mut S,
    action_collector: &'a mut ActionCollector,
    is_leader: bool,
}

impl EventHandler for AppendJournalEntry<GetStateJournalEntry> {
    async fn handle(self, ctx: Context) -> Result<(), Error> {

    }
}

pub(crate) struct StateMachineApplyContext<'a, S> {
    storage: &'a mut S,
    action_collector: &'a mut ActionCollector,
    is_leader: bool,
}

trait FromContext {
    fn from_context(context: &Context) -> Self;
}

trait Handler<T> {
    fn call(self, context: Context);
}

impl<F, T> Handler<T> for F
where
    F: Fn(T),
    T: FromContext,
{
    fn call(self, context: Context) {
        (self)(T::from_context(&context));
    }
}

impl<T1, T2, F> Handler<(T1, T2)> for F
    where
        F: Fn(T1, T2),
        T1: FromContext,
        T2: FromContext,
    {
        fn call(self, context: Context) {
            (self)(T1::from_context(&context), T2::from_context(&context));
        }
    }

pub fn trigger<T, H>(context: Context, handler: H)
where
    H: Handler<T>,
{
    handler.call(context);
}