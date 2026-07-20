// Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{VQueuesMeta, VQueuesMetaCache};

/// Grants access to a read-only view of the VQueues metadata.
pub trait HasVQueues {
    /// Read access to vqueues' metadata.
    fn vqueues(&self) -> VQueuesMeta<'_>;
}

/// Grants read-write access to the VQueues metadata.
pub trait HasVQueuesMut: HasVQueues {
    /// Access to mutate vqueues' metadata.
    fn vqueues_mut(&mut self) -> &mut VQueuesMetaCache;
}

// -- Boilerplate --
impl<P: HasVQueues> HasVQueues for &P {
    #[inline]
    fn vqueues(&self) -> VQueuesMeta<'_> {
        (**self).vqueues()
    }
}

impl<P: HasVQueues> HasVQueues for &mut P {
    #[inline]
    fn vqueues(&self) -> VQueuesMeta<'_> {
        (**self).vqueues()
    }
}

impl<P: HasVQueuesMut> HasVQueuesMut for &mut P {
    #[inline]
    fn vqueues_mut(&mut self) -> &mut VQueuesMetaCache {
        (**self).vqueues_mut()
    }
}
