#!/usr/bin/env node

/*
 * Copyright (c) 2024 - Restate Software, Inc., Restate GmbH
 *
 * This file is part of the Restate load test environment,
 * which is released under the MIT license.
 *
 * You can find a copy of the license in file LICENSE in the
 * scripts/loadtest-environment directory of this repository, or at
 * https://github.com/restatedev/retate/blob/main/scripts/loadtest-environment/LICENSE
 */

import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import { LoadTestEnvironmentStack } from "../lib/loadtest-environment-stack";

// Graviton2 arm64, 8vCPU x 32GB RAM
const INSTANCE_TYPE_MID = ec2.InstanceType.of(ec2.InstanceClass.M6G, ec2.InstanceSize.XLARGE2);

// Xeon x86_64, 32vCPU x64GB RAM, 1.9TB local NVMe SSD
const INSTANCE_TYPE_HIGH = ec2.InstanceType.of(ec2.InstanceClass.C6ID, ec2.InstanceSize.XLARGE8);

const EBS_VOLUME_MID = {
  volumeType: ec2.EbsDeviceVolumeType.GP3,
  volumeSize: 16, // GiB
  deleteOnTermination: true,
  iops: 3_000,
  throughput: 250, // MiB/s
};

const EBS_VOLUME_HIGH = {
  volumeType: ec2.EbsDeviceVolumeType.GP3,
  volumeSize: 64, // GiB
  deleteOnTermination: true,
  iops: 16_000,
  throughput: 1_000, // MiB/s
};

const EBS_VOLUME_ULTRA = {
  volumeType: ec2.EbsDeviceVolumeType.IO2,
  volumeSize: 64, // GiB
  deleteOnTermination: true,
  iops: 64_000,
};

const app = new cdk.App();

new LoadTestEnvironmentStack(app, `restate-benchmark-sandbox-${process.env.USER}`, {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
  instanceType: INSTANCE_TYPE_MID,
  vpcId: undefined, // use default VPC unless specified
  ebsVolume: EBS_VOLUME_MID, // optional EBS volume
});
