# Restate Load Test Sandbox

This CDK project creates reproducible environments for testing Restate performance.

You will need an AWS account, bootstrapped for CDK use, and npm. Run `npm install` to fetch the required dependencies.

Before you deploy the stack, you may want to customize the instance type and storage configuration in `bin/loadtest-env.ts`.

Run `npx cdk deploy` to create the EC2 instance. Currently this stack does not automatically download the Restate server, it only sets up the environment for building it from source - or running a prebuilt release.

To connect to your EC2 instance, use `aws ssm start-session --target <instance-id>`. You can find some predefined load tests in the `tests` directory. You can find some standard restate-server configurations in the `config` directory.

If you want to temporarily suspend the machine, you can do so with `aws ec2 stop-instances --instance-ids <instance-id>` and later re-start it. You will still pay for the associated EBS volumes even while the instance is stopped. Note that the contents of any local NVMe devices will be wiped if you do so. Tear down the stack using `npx cdk destroy` when you are done with it.
