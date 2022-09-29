# zipper-s3
[![Circle CI](https://circleci.com/gh/Financial-Times/zipper-s3/tree/master.png?style=shield)](https://circleci.com/gh/Financial-Times/zipper-s3/tree/master) [![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/zipper-s3)](https://goreportcard.com/report/github.com/Financial-Times/zipper-s3) [![Coverage Status](https://coveralls.io/repos/github/Financial-Times/zipper-s3/badge.svg)](https://coveralls.io/github/Financial-Times/zipper-s3)
## Introduction

UPP Golang App to download files from S3, zip them and upload the newly created zip file back to S3

## Installation

Download the source code, dependencies and test dependencies:

        go get -u github.com/Financial-Times/zipper-s3
        cd $GOPATH/src/github.com/Financial-Times/zipper-s3
        go build .

## Running locally

1. Run the tests and install the binary:

        go test ./... -v -race
        go install

2. Run the binary (using the `help` flag to see the available optional arguments):
    Environment variables:
    - `IS_ENABLED` flag which if it is true, the app will run the zip creation process, otherwise will stop immediately after start.
    - `MAX_NO_OF_GOROUTINES` the maximum number of goroutines which is used to zip files
    - `YEAR_TO_START` the app will create yearly zips starting from provided year. Defaults to 1995, when the first FT article has been published. 
    - `BUCKET_NAME` bucket name of content
    - `BUCKET_REGION` bucket-name's region
    - `S3_DOMAIN` S3 domain of content
    - `S3_CONTENT_FOLDER` name of the folder that json files with the content are stored in
    - `S3_CONCEPT_FOLDER` name of the folder that json files with the concept are stored in
    - `LOG_DEBUG` flag which if it is set to true, the app will also output debug logs

    AWS related envvars.
    - `AWS_ACCESS_KEY_ID` S3 access key
    - `AWS_SECRET_ACCESS_KEY` S3 secret key
    - `AWS_REGION` S3 region

## Running in Kubernetes

When the app is running in kubernetes, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` envvars are not being used, instead `AWS_ROLE_ARN` and `AWS_WEB_IDENTITY_TOKEN_FILE` are used. The `aws-sdk-go` uses whichever envvars are present behind the scenes(in our code base there isn't logic for this).

## Testing

This service is executed as a daily Kubernetes cronjob in the UPP Publishing clusters.
It takes around 2 hours to complete its execution.
If you don't want to wait until the next cron execution time to check the logs and the files in S3,
you can manually trigger an execution using the following command (wait 2 hours to see if all archives are uploaded successfully):

```shell
kubectl create job --from=cronjob/zipper-s3 zipper-s3-manual
```

## Build and deployment

* Built by Docker Hub on merge to master: [coco/zipper-s3](https://hub.docker.com/r/coco/zipper-s3/)
* CI provided by CircleCI: [zipper-s3](https://circleci.com/gh/Financial-Times/zipper-s3)
