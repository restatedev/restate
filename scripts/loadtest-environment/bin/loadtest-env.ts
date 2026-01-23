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

import "source-map-support/register";
import * as cdk from "aws-cdk-lib";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import { LoadTestEnvironmentStack } from "../lib/loadtest-environment-stack";

// Graviton2 arm64, 8vCPU x 32GB RAM
const INSTANCE_TYPE_MID = ec2.InstanceType.of(ec2.InstanceClass.M6G, ec2.InstanceSize.XLARGE2);

// Xeon x86_64 4vCPU x 8GB RAM
const INSTANCE_TYPE_X86_4_VCPU_8GB_RAM = ec2.InstanceType.of(ec2.InstanceClass.C6I, ec2.InstanceSize.XLARGE);

// Xeon x86_64, 32vCPU x64GB RAM, 1.9TB local NVMe SSD
const INSTANCE_TYPE_HIGH = ec2.InstanceType.of(ec2.InstanceClass.C6ID, ec2.InstanceSize.XLARGE8);

const EBS_VOLUME_MID = {
  volumeType: ec2.EbsDeviceVolumeType.GP3,
  volumeSize: 64, // GiB
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

new LoadTestEnvironmentStack(app, `restate-ebs-testbed`, {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
  instanceType: INSTANCE_TYPE_X86_4_VCPU_8GB_RAM,
  vpcId: undefined, // use default VPC unless specified
  ebsVolume: EBS_VOLUME_MID, // optional EBS volume
});
