#!/usr/bin/env node

/*
 * Copyright (c) 2023 - 2026 Restate Software, Inc., Restate GmbH.
 * All rights reserved.
 *
 * Use of this software is governed by the Business Source License
 * included in the LICENSE file.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0.
 */

import { spawnSync } from "child_process";
import os from "node:os";

function getExePath() {
  const arch = os.arch();
  const op = os.platform();

  try {
    return require.resolve(`@restatedev/restate-${op}-${arch}/bin/restate`);
  } catch (e) {
    throw new Error(
      `Couldn't find application binary inside node_modules for ${op}-${arch}`,
    );
  }
}

function run() {
  const args = process.argv.slice(2);
  const processResult = spawnSync(getExePath(), args, { stdio: "inherit" });
  process.exit(processResult.status ?? 0);
}

run();
