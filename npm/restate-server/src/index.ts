#!/usr/bin/env node

import { spawnSync } from "child_process";
import os from 'node:os';

function getExePath() {
  const arch = os.arch();
  const op = os.platform();

  try {
    return require.resolve(`@restatedev/restate-server-${op}-${arch}/bin/restate-server`);
  } catch (e) {
    throw new Error(
      `Couldn't find application binary inside node_modules for ${op}-${arch}`
    );
  }
}

function run() {
  const args = process.argv.slice(2);
  const env = {
    RESTATE_META__STORAGE_PATH: ".restate/meta/",
    RESTATE_WORKER__STORAGE_ROCKSDB__PATH: ".restate/db/",
    ...process.env
  }
  const processResult = spawnSync(getExePath(), args, {stdio: "inherit", env});
  process.exit(processResult.status ?? 0);
}

run();
