# Multi-region with Restate OSS

The attached helm values files give a base config for a three-region Restate cluster. Two regions run partition processors,
while a third only runs log and metadata roles in order to help the other regions form a quorum.

## Prerequisites

### Kubernetes Clusters
Three Kubernetes clusters across different regions. If you like, you can use less clusters (eg, just one) and use separate namespaces, as a way to test out the config.

### Networking
All three Restate nodes need to reach each other's 5122 port. This can go through an NLB if they are in different Kubernetes
clusters.
The easiest way to create a new NLB is likely by creating a LoadBalancer type Service in each namespace, but this may vary depending on your EKS setup:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: restate-node
  annotations:
    service.beta.kubernetes.io/aws-load-balancer-type: external
    service.beta.kubernetes.io/aws-load-balancer-nlb-target-type: ip
spec:
  ports:
  - name: node
    port: 5122
    protocol: TCP
    targetPort: 5122
  # this is important!
  publishNotReadyAddresses: true
  type: LoadBalancer
  selector:
    app: restate
```

`publishNotReadyAddresses` must be set to true for any service that is used for node-to-node traffic; we want nodes to be able to find each other
even if they are not ready, to avoid bootstrap problems when bringing up a cluster that is currently down.

The globally-resolveable and reachable dns name (with protocol `http` and port `5122`) should be set as `RESTATE_ADVERTISED_ADDRESS`, and all the nodes' `RESTATE_ADVERTISED_ADDRESS` must also be listed in `RESTATE_METADATA_CLIENT__ADDRESSES`.

### S3 Bucket
By the nature of S3, the snapshot bucket must be in a single region - see below for a discussion of the implications of this.

The region of the S3 bucket must be specified as `RESTATE_WORKER__SNAPSHOTS__AWS_REGION` and the name (and subpath, if any) as `RESTATE_WORKER__SNAPSHOTS__DESTINATION` in the non-witness regions.

### IAM roles
A single IAM role can be used for all three Restate nodes if desired. It needs permissions to read and write objects in the S3 bucket:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": "s3:ListBucket",
            "Resource": [
                "arn:aws:s3:::$bucket_name"
            ],
            "Effect": "Allow"
        },
        {
            "Action": [
                "s3:PutObject",
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::$bucket_name/*"
            ],
            "Effect": "Allow"
        }
    ]
}
```

The role needs an appropriate trust policy such that each ServiceAccount can assume it (one statement for each of the two non-witness regions). With
a standard EKS IRSA setup, this looks like the following:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::$account_id:oidc-provider/$oidc_provider_region_one"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "$oidc_provider_region_one:aud": "sts.amazonaws.com",
          "$oidc_provider_region_one:sub": "system:serviceaccount:$namespace_region_one:restate"
        }
      }
    },
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::$account_id:oidc-provider/$oidc_provider_region_two"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "$oidc_provider_region_two:aud": "sts.amazonaws.com",
          "$oidc_provider_region_two:sub": "system:serviceaccount:$namespace_region_two:restate"
        }
      }
    }
  ]
}
```

You can get your $oidc_provider with `aws eks describe-cluster --name my-cluster --query "cluster.identity.oidc.issuer" --output text | sed -e "s/^https:\/\///"`.

The name of the role should be inserted into all 3 values files as the `eks.amazonaws.com/role-arn` annotation.

## Deploying
Create the AWS resources as above, edit your values files appropriately, and:
```bash
# in the first cluster
helm upgrade --install restate oci://ghcr.io/restatedev/restate-helm --version 1.3.2 --namespace restate-node1 --create-namespace -f ./values-node1.yaml
# in the second cluster
helm upgrade --install restate oci://ghcr.io/restatedev/restate-helm --version 1.3.2 --namespace restate-node2 --create-namespace -f ./values-node2.yaml
# in the third cluster
helm upgrade --install restate oci://ghcr.io/restatedev/restate-helm --version 1.3.2 --namespace restate-node3 --create-namespace -f ./values-node3.yaml
```

### Caveats

- The current version of the helm chart uses a :8080/restate/health health check, which node3, as a witness, does not serve.
  This pod will not appear ready, but will otherwise function fine. This will be fixed in the next release.
- Both non-witness regions will use a snapshot bucket in a single region.
  However, Restate does not rely on the bucket being up all the time, so even if the region has been failed over, the cluster can still function.
  The bucket primarily is needed to bring up new nodes in the case of EBS volume loss, which is a rare operation.
  Even so, this single-region limitation will be lifted in a future release.
