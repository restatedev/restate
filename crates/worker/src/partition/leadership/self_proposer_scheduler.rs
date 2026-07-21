use enum_map::Enum;

#[derive(Debug, Clone, Enum, Copy)]
pub(crate) enum SelfProposerSchedulerFlow {
    Invoker,
    Timer,
    Shuffle,
    Cleaner,
    UpsertSchema,
    UpsertRuleBook,
    NetworkService,
    PartitionMaintenance,
    Scheduler,
}

pub(crate) struct SelfProposerScheduler {
    cursor: usize,
}

impl SelfProposerScheduler {
    const ALL_FLOWS: [SelfProposerSchedulerFlow; 9] = [
        SelfProposerSchedulerFlow::Invoker,
        SelfProposerSchedulerFlow::Timer,
        SelfProposerSchedulerFlow::Shuffle,
        SelfProposerSchedulerFlow::Cleaner,
        SelfProposerSchedulerFlow::UpsertSchema,
        SelfProposerSchedulerFlow::UpsertRuleBook,
        SelfProposerSchedulerFlow::NetworkService,
        SelfProposerSchedulerFlow::PartitionMaintenance,
        SelfProposerSchedulerFlow::Scheduler,
    ];

    pub(crate) fn new() -> SelfProposerScheduler {
        SelfProposerScheduler { cursor: 0 }
    }

    // pub fn next_flow(&mut self) -> SelfProposerSchedulerFlow {
    //     let ret = SelfProposerScheduler::ALL_FLOWS[self.cursor];
    //     self.cursor = (self.cursor + 1) % SelfProposerScheduler::ALL_FLOWS.len();
    //     ret
    // }

    pub fn scan_order(&mut self) -> [SelfProposerSchedulerFlow; 9] {
        let mut ret = SelfProposerScheduler::ALL_FLOWS;
        ret.rotate_left(self.cursor);
        self.cursor = (self.cursor + 1) % SelfProposerScheduler::ALL_FLOWS.len();
        ret
    }
}
