#[derive(Debug)]
pub(in crate::service) enum InvokerConcurrencyQuota {
    Unlimited,
    Limited { available_slots: usize },
}

impl InvokerConcurrencyQuota {
    pub(in crate::service) fn new(quota: Option<usize>) -> Self {
        match quota {
            Some(available_slots) => Self::Limited { available_slots },
            None => Self::Unlimited,
        }
    }

    pub(in crate::service) fn is_slot_available(&self) -> bool {
        match self {
            Self::Unlimited => true,
            Self::Limited { available_slots } => *available_slots > 0,
        }
    }

    pub(in crate::service) fn unreserve_slot(&mut self) {
        match self {
            Self::Unlimited => {}
            Self::Limited { available_slots } => *available_slots += 1,
        }
    }

    pub(in crate::service) fn reserve_slot(&mut self) {
        debug_assert!(self.is_slot_available());
        match self {
            Self::Unlimited => {}
            Self::Limited { available_slots } => *available_slots -= 1,
        }
    }
}
