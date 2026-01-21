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

    const keyPair = new ec2.KeyPair(this, "SshKeypair");
    const instanceRole = new iam.Role(this, "InstanceRole", {
      assumedBy: new iam.ServicePrincipal("ec2.amazonaws.com"),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonSSMManagedInstanceCore")],
    });
    const instanceProfile = new iam.InstanceProfile(this, "InstanceProfile", { role: instanceRole });

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
      "set -eufx -o pipefail",
      "apt-get update",
      "apt-get install -y make cmake clang protobuf-compiler rustup npm wrk tmux htop podman podman-docker jq prometheus-node-exporter",
      "mkdir /restate-data",
    );

    const cloudConfig = ec2.UserData.custom([`cloud_final_modules:`, `- [scripts-user, once]`].join("\n"));
    const userData = new ec2.MultipartUserData();
    userData.addUserDataPart(cloudConfig, "text/cloud-config");
    userData.addUserDataPart(initScript, "text/x-shellscript");

    const vpc = ec2.Vpc.fromLookup(this, "Vpc", { isDefault: true });
    const securityGroup = new ec2.SecurityGroup(this, "SecurityGroup", {
      vpc,
      description: "Restate servers",
    });

    const nodes: ec2.Instance[] = [];
    const subnets = vpc.publicSubnets;
    for (let n = 1; n <= 3; n++) {
      const node = new ec2.Instance(this, `N${n}`, {
        instanceProfile,
        vpc,
        vpcSubnets: {
          subnets: [subnets[n % subnets.length]],
        },
        instanceType: props.instanceType,
        machineImage: ec2.MachineImage.fromSsmParameter(
          `/aws/service/canonical/ubuntu/server/24.04/stable/current/${
            props.instanceType.architecture == ec2.InstanceArchitecture.X86_64
              ? "amd64"
              : props.instanceType.architecture
          }/hvm/ebs-gp3/ami-id`,
        ),
        blockDevices: [
          {
            deviceName: "/dev/sda1",
            volume: ec2.BlockDeviceVolume.ebs(200, {
              volumeType: ec2.EbsDeviceVolumeType.GP2,
            }),
          },
        ],
        userData,
        keyPair,
      });
      node.addSecurityGroup(securityGroup);
      new cdk.CfnOutput(this, `InstanceId${n}`, { value: node.instanceId });
      new cdk.CfnOutput(this, `Node${n}`, { value: node.instancePublicDnsName });
      new cdk.CfnOutput(this, `NodeInternalIpAddress${n}`, { value: node.instancePrivateIp });
      nodes.push(node); // NB: they will be zero-based here!
    }

    // Cloud init script. To run on every boot, make sure it's idempotent and change `once` to `always` below.
    const grafanaInitScript = ec2.UserData.forLinux();
    grafanaInitScript.addCommands(
      "set -eufx -o pipefail",
      "wget -q -O - https://apt.grafana.com/gpg.key | gpg --dearmor > /etc/apt/trusted.gpg.d/grafana.gpg",
      'add-apt-repository -y "deb https://packages.grafana.com/oss/deb stable main"',
      "apt-get update",
      "apt-get install -y grafana prometheus",
      "",
      "cat << EOF >> /etc/prometheus/prometheus.yml",
      `
  - job_name: 'restate-cluster-nodes'
    scrape_interval: 5s
    static_configs:
      - targets: ['${nodes[0].instancePrivateIp}:9100']
      - targets: ['${nodes[1].instancePrivateIp}:9100']
      - targets: ['${nodes[2].instancePrivateIp}:9100']

  - job_name: 'restate-cluster-restate'
    metrics_path: '/metrics'
    scrape_interval: 5s
    static_configs:
      - targets: ['${nodes[0].instancePrivateIp}:5122']
      - targets: ['${nodes[1].instancePrivateIp}:5122']
      - targets: ['${nodes[2].instancePrivateIp}:5122']
`,
      "EOF",
      "",
      "systemctl restart grafana-server",
      "systemctl restart prometheus",
    );

    const grafanaCloudConfig = ec2.UserData.custom([`cloud_final_modules:`, `- [scripts-user, once]`].join("\n"));
    const grafanaUserData = new ec2.MultipartUserData();
    grafanaUserData.addUserDataPart(grafanaCloudConfig, "text/cloud-config");
    grafanaUserData.addUserDataPart(grafanaInitScript, "text/x-shellscript");

    const grafanaInstanceType = ec2.InstanceType.of(ec2.InstanceClass.T4G, ec2.InstanceSize.LARGE);
    const grafana = new ec2.Instance(this, `GrafanaInstance`, {
      instanceProfile,
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      instanceType: grafanaInstanceType,
      machineImage: ec2.MachineImage.fromSsmParameter(
        `/aws/service/canonical/ubuntu/server/24.04/stable/current/${
          grafanaInstanceType.architecture == ec2.InstanceArchitecture.X86_64
            ? "amd64"
            : grafanaInstanceType.architecture
        }/hvm/ebs-gp3/ami-id`,
      ),
      blockDevices: [
        {
          deviceName: "/dev/sda1", // root device
          volume: ec2.BlockDeviceVolume.ebs(100),
        },
      ],
      userData: grafanaUserData,
      keyPair,
    });
    grafana.addSecurityGroup(securityGroup);
    new cdk.CfnOutput(this, "GrafanaNode", { value: grafana.instancePublicDnsName });
    new cdk.CfnOutput(this, "GrafanaInstanceId", { value: grafana.instanceId });
    new cdk.CfnOutput(this, "GrafanaAddress", { value: `http://${grafana.instancePublicDnsName}:3000` });

    securityGroup.addIngressRule(securityGroup, ec2.Port.allTraffic());
    securityGroup.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.allTraffic());

    new cdk.CfnOutput(this, "KeyArn", { value: keyPair.privateKey.parameterArn });
  }
}
