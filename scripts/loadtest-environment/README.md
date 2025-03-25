# Restate Load Test Sandbox

This CDK project creates reproducible environments for testing Restate performance.

You will need an AWS account, bootstrapped for CDK use, and npm. Run `npm install` to fetch the required dependencies.

Before you deploy the stack, you may want to customize the instance type and storage configuration in
`bin/loadtest-env.ts`.

Run `npx cdk deploy` to create the EC2 instance. Currently this stack does not automatically download the Restate
server, it only sets up the environment for building it from source - or running a prebuilt release. Be aware that
the user-scripts in the stack's EC2 instances only run once, and CloudFormation may replace them if you change them.

This will create a file `cdk-outputs.json` which contains some useful output parameters like the instance ids and
addresses. The `get-node-info.sh` script will extract the SSH private key and nodes list from CloudFormation.

To connect to your EC2 instance, you can use `ssh -i private-key.pem -l ubuntu ${node-public-ip}` or alternatively
`aws ssm start-session --target <instance-id>`. You can find some predefined load tests in the `tests` directory. You
can find some standard restate-server configurations in the `config` directory.

The Grafana default username / password will be `admin` / `admin`.

If you want to temporarily suspend some machines, you can do so with
`aws ec2 stop-instances --instance-ids <instance-id>` and later re-start it. You will still pay for the associated EBS
volumes even while the instance is stopped. Note that the contents of any local NVMe devices will be wiped if you do so.
Tear down the stack using `npx cdk destroy` when you are done with it.
