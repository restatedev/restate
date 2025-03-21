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
      "apt update && apt upgrade",
      "apt install -y make cmake clang protobuf-compiler rustup npm wrk tmux htop podman podman-docker jq prometheus-node-exporter",
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
          // {
          //   deviceName: "/dev/sdb",
          //   volume: {
          //     ebsDevice: props.ebsVolume,
          //     virtualName: "restate-data",
          //   },
          // },
        ],
        userData,
        keyPair,
      });
      node.addSecurityGroup(securityGroup);
      new cdk.CfnOutput(this, `InstanceId${n}`, { value: node.instanceId });
      new cdk.CfnOutput(this, `Node${n}`, { value: node.instancePublicDnsName });
    }

    /*
      wget -q -O - https://packages.grafana.com/gpg.key | sudo apt-key add -
      add-apt-repository "deb https://packages.grafana.com/oss/deb stable main"
      apt update && apt install grafana prometheus

      cat > /etc/prometheus/prometheus.yml <<EOF

        - job_name: 'restate-cluster-nodes'
          scrape_interval: 5s
          static_configs:
            - targets: ['172.31.34.171:9100']
            - targets: ['172.31.1.249:9100']
            - targets: ['172.31.28.10:9100']

        - job_name: 'restate-cluster-restate'
          metrics_path: '/metrics'
          scrape_interval: 5s
          static_configs:
            - targets: ['172.31.34.171:5122']
            - targets: ['172.31.1.249:5122']
            - targets: ['172.31.28.10:5122']
      EOF
      systemctl restart prometheus
    */
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
      keyPair,
    });
    grafana.addSecurityGroup(securityGroup);

    securityGroup.addIngressRule(securityGroup, ec2.Port.allTraffic());
    securityGroup.addIngressRule(ec2.Peer.anyIpv4(), ec2.Port.allTraffic());

    new cdk.CfnOutput(this, "KeyArn", { value: keyPair.privateKey.parameterArn });
  }
}
