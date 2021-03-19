<!--
    Written in the format prescribed by https://github.com/Financial-Times/runbook.md.
    Any future edits should abide by this format.
-->
# Zipper S3

App that is zipping up content and concepts from an S3 bucket and uploads the zip file back into the bucket.

## Code

zipper-s3

<!-- Placeholder - remove HTML comment markers to activate
## Primary URL
Enter descriptive text satisfying the following:
The main url served by the system.

...or delete this placeholder if not applicable to this system
-->

## Service Tier

Bronze

## Lifecycle Stage

Production

## Host Platform

AWS

## Architecture

UPP Golang App to download files from S3, zip them and upload the newly created zip file back to S3. This service is executed as a daily Kubernetes cronjob in the UPP Publishing clusters.

## Contains Personal Data

No

## Contains Sensitive Data

No

<!-- Placeholder - remove HTML comment markers to activate
## Can Download Personal Data
Choose Yes or No

...or delete this placeholder if not applicable to this system
-->

<!-- Placeholder - remove HTML comment markers to activate
## Can Contact Individuals
Choose Yes or No

...or delete this placeholder if not applicable to this system
-->

## Failover Architecture Type

ActiveActive

## Failover Process Type

FullyAutomated

## Failback Process Type

FullyAutomated

## Failover Details

The cronjob is deployed in both Publish clusters. The failover guide for the cluster is located here: <https://github.com/Financial-Times/upp-docs/tree/master/failover-guides/publishing-cluster>

## Data Recovery Process Type

NotApplicable

## Data Recovery Details

NotApplicable

## Release Process Type

PartiallyAutomated

## Rollback Process Type

Manual

## Release Details

It is being deployed via a Jenkins job as the services are. No failover is required as it is a Cronjob.

<!-- Placeholder - remove HTML comment markers to activate
## Heroku Pipeline Name
Enter descriptive text satisfying the following:
This is the name of the Heroku pipeline for this system. If you don't have a pipeline, this is the name of the app in Heroku. A pipeline is a group of Heroku apps that share the same codebase where each app in a pipeline represents the different stages in a continuous delivery workflow, i.e. staging, production.

...or delete this placeholder if not applicable to this system
-->

## Key Management Process Type

Manual

## Key Management Details

To access the job clients need to provide basic auth credentials to log into the k8s clusters. To rotate credentials you need to login to a particular cluster and update varnish-auth secrets.

## Monitoring

NotApplicable

## First Line Troubleshooting

<https://github.com/Financial-Times/upp-docs/tree/master/guides/ops/first-line-troubleshooting>

## Second Line Troubleshooting

Please refer to the GitHub repository README for troubleshooting information.