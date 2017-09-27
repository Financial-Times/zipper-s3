# zipper-s3
[![Circle CI](https://circleci.com/gh/Financial-Times/zipper-s3/tree/master.png?style=shield)](https://circleci.com/gh/Financial-Times/zipper-s3/tree/master)[![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/zipper-s3)](https://goreportcard.com/report/github.com/Financial-Times/zipper-s3) [![Coverage Status](https://coveralls.io/repos/github/Financial-Times/zipper-s3/badge.svg)](https://coveralls.io/github/Financial-Times/zipper-s3)
## Introduction

UPP Golang App to download files from S3, zip them and upload the newly created zip file back to S3

## Installation

Download the source code, dependencies and test dependencies:

        go get -u github.com/kardianos/govendor
        go get -u github.com/Financial-Times/zipper-s3
        cd $GOPATH/src/github.com/Financial-Times/zipper-s3
        govendor sync
        go build .

## Running locally

1. Run the tests and install the binary:

        govendor sync
        govendor test -v -race
        go install

2. Run the binary (using the `help` flag to see the available optional arguments):
    Environment variables:
    - `YEAR_TO_START` the app will create yearly zips starting from provided year. Defaults to 1995, when the first FT article has been published. 
    - `AWS_ACCESS_KEY_ID` S3 access key
    - `AWS_SECRET_ACCESS_KEY` S3 secret key
    - `BUCKET_NAME` bucket name of content
    - `S3_DOMAIN` S3 domain of content
    - `S3_CONTENT_FOLDER` name of the folder that json files with the content are stored in.

        $GOPATH/bin/zipper-s3 [--help]
        


## Build and deployment

* Built by Docker Hub on merge to master: [coco/zipper-s3](https://hub.docker.com/r/coco/zipper-s3/)
* CI provided by CircleCI: [zipper-s3](https://circleci.com/gh/Financial-Times/zipper-s3)
