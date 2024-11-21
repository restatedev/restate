/*
 * Copyright (c) 2023 - 2025 Restate Software, Inc., Restate GmbH.
 * All rights reserved.
 *
 * Use of this software is governed by the Business Source License
 * included in the LICENSE file.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0.
 */

import * as cdk from "aws-cdk-lib";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as iam from "aws-cdk-lib/aws-iam";
import { Construct } from "constructs";

export interface LoadTestEnvironmentStackProps extends cdk.StackProps {
  instanceType: ec2.InstanceType;
  vpcId: string | undefined;
  ebsVolume: ec2.EbsDeviceProps | undefined;
}

export class LoadTestEnvironmentStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: LoadTestEnvironmentStackProps) {
    super(scope, id, props);

    const instanceRole = new iam.Role(this, "InstanceRole", {
      assumedBy: new iam.ServicePrincipal("ec2.amazonaws.com"),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonSSMManagedInstanceCore")],
    });

    const invokerRole = new iam.Role(this, "InvokerRole", {
      assumedBy: instanceRole,
    });

    new iam.Policy(this, "AssumeInvokerRolePolicy", {
      statements: [
        new iam.PolicyStatement({
          sid: "AllowAssumeInvokerRole",
          actions: ["sts:AssumeRole"],
          resources: [invokerRole.roleArn],
        }),
      ],
    }).attachToRole(instanceRole);

    // Cloud init script. To run on every boot, make sure it's idempotent and change `once` to `always` below.
    const initScript = ec2.UserData.forLinux();
    initScript.addCommands(
      "set -euf -o pipefail",
      "apt update && apt upgrade",
      "apt install -y make cmake clang protobuf-compiler npm wrk tmux htop",
    );
    const cloudConfig = ec2.UserData.custom([`cloud_final_modules:`, `- [scripts-user, once]`].join("\n"));

    const userData = new ec2.MultipartUserData();
    userData.addUserDataPart(cloudConfig, "text/cloud-config");
    userData.addUserDataPart(initScript, "text/x-shellscript");

    const vpc = ec2.Vpc.fromLookup(this, "Vpc", { isDefault: true });
    const testInstance = new ec2.Instance(this, "TestInstance", {
      vpc,
      // Make sure to use an available subnet for the VPC.
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      instanceType: props.instanceType,
      machineImage: ec2.MachineImage.fromSsmParameter(
        `/aws/service/canonical/ubuntu/server/24.04/stable/current/${
          props.instanceType.architecture == ec2.InstanceArchitecture.X86_64 ? "amd64" : props.instanceType.architecture
        }/hvm/ebs-gp3/ami-id`,
      ),
      role: instanceRole,
      blockDevices: [
        {
          deviceName: "/dev/sde", // "e" for EBS
          volume: {
            ebsDevice: props.ebsVolume,
            virtualName: "restate-data",
          },
        },
      ],
      userData,
    });

    // In case you might want to enable remote access from within the VPC
    const ingressSecurityGroup = new ec2.SecurityGroup(this, "IngressSecurityGroup", {
      vpc,
      description: "Restate Ingress ACLs",
    });
    testInstance.addSecurityGroup(ingressSecurityGroup);
    const adminSecurityGroup = new ec2.SecurityGroup(this, "AdminSecurityGroup", {
      vpc,
      description: "Restate Admin ACLs",
    });
    testInstance.addSecurityGroup(adminSecurityGroup);

    new cdk.CfnOutput(this, "InstanceId", { value: testInstance.instanceId });
  }
}
