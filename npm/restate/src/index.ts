#!/usr/bin/env node

import { spawnSync } from "child_process";
import os from 'node:os';

function getExePath() {
  const arch = os.arch();
  const op = os.platform();

  try {
    return require.resolve(`@restatedev/restate-${op}-${arch}/bin/restate`);
  } catch (e) {
    throw new Error(
      `Couldn't find application binary inside node_modules for ${op}-${arch}`
    );
  }
}

function run() {
  const args = process.argv.slice(2);
  const processResult = spawnSync(getExePath(), args, { stdio: "inherit" });
  process.exit(processResult.status ?? 0);
}

run();
