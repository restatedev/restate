#[derive(Debug)]
pub(super) enum InvokerConcurrencyQuota {
    Unlimited,
    Limited { available_slots: usize },
}

impl InvokerConcurrencyQuota {
    pub(super) fn new(quota: Option<usize>) -> Self {
        match quota {
            Some(available_slots) => Self::Limited { available_slots },
            None => Self::Unlimited,
        }
    }

    pub(super) fn is_slot_available(&self) -> bool {
        match self {
            Self::Unlimited => true,
            Self::Limited { available_slots } => *available_slots > 0,
        }
    }

    pub(super) fn unreserve_slot(&mut self) {
        match self {
            Self::Unlimited => {}
            Self::Limited { available_slots } => *available_slots += 1,
        }
    }

    pub(super) fn reserve_slot(&mut self) {
        assert!(self.is_slot_available());
        match self {
            Self::Unlimited => {}
            Self::Limited { available_slots } => *available_slots -= 1,
        }
    }
}
