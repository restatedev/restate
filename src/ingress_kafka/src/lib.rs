// Copyright (c) 2023 -  Restate Software, Inc., Restate GmbH.
// All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod consumer_task;
mod options;
mod subscription_controller;

use tokio::sync::mpsc;

pub use options::{
    KafkaClusterOptions, Options, OptionsBuilder, OptionsBuilderError, ValidationError,
};
pub use subscription_controller::{Command, Error, Service};

pub type SubscriptionCommandSender = mpsc::Sender<Command>;
pub type SubscriptionCommandReceiver = mpsc::Receiver<Command>;
