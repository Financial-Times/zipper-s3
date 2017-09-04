# zipper-s3
_Should be the same as the github repo name but it isn't always._

## Introduction

_What is this service and what is it for? What other services does it depend on_

UPP Golang App to download files from S3, zip them and upload the newly created zip file back to S3

## Installation
      
_How can I install it_

Download the source code, dependencies and test dependencies:

        go get -u github.com/kardianos/govendor
        go get -u github.com/Financial-Times/zipper-s3
        cd $GOPATH/src/github.com/Financial-Times/zipper-s3
        govendor sync
        go build .

## Running locally
_How can I run it_

1. Run the tests and install the binary:

        govendor sync
        govendor test -v -race
        go install

2. Run the binary (using the `help` flag to see the available optional arguments):

        $GOPATH/bin/zipper-s3 [--help]

Options:

        --app-system-code="zipper-s3"            System Code of the application ($APP_SYSTEM_CODE)
        --app-name="zipper-s3"                   Application name ($APP_NAME)
        --port="8080"                                           Port to listen on ($APP_PORT)
        

## Build and deployment
_How can I build and deploy it (lots of this will be links out as the steps will be common)_

* Built by Docker Hub on merge to master: [coco/zipper-s3](https://hub.docker.com/r/coco/zipper-s3/)
* CI provided by CircleCI: [zipper-s3](https://circleci.com/gh/Financial-Times/zipper-s3)

## Other information
_Anything else you want to add._

_e.g. (NB: this example may be something we want to extract as it's probably common to a lot of services)_

### Logging

* The application uses [logrus](https://github.com/sirupsen/logrus); the log file is initialised in [main.go](main.go).
* Logging requires an `env` app parameter, for all environments other than `local` logs are written to file.
* When running locally, logs are written to console. If you want to log locally to file, you need to pass in an env parameter that is != `local`.