# zipper-s3
_Should be the same as the github repo name but it isn't always._

[![Circle CI](https://circleci.com/gh/Financial-Times/zipper-s3/tree/master.png?style=shield)](https://circleci.com/gh/Financial-Times/zipper-s3/tree/master)[![Go Report Card](https://goreportcard.com/badge/github.com/Financial-Times/zipper-s3)](https://goreportcard.com/report/github.com/Financial-Times/zipper-s3) [![Coverage Status](https://coveralls.io/repos/github/Financial-Times/zipper-s3/badge.svg)](https://coveralls.io/github/Financial-Times/zipper-s3)

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
        
3. Test:

    1. Either using curl:

            curl http://localhost:8080/people/143ba45c-2fb3-35bc-b227-a6ed80b5c517 | json_pp

    1. Or using [httpie](https://github.com/jkbrzt/httpie):

            http GET http://localhost:8080/people/143ba45c-2fb3-35bc-b227-a6ed80b5c517

## Build and deployment
_How can I build and deploy it (lots of this will be links out as the steps will be common)_

* Built by Docker Hub on merge to master: [coco/zipper-s3](https://hub.docker.com/r/coco/zipper-s3/)
* CI provided by CircleCI: [zipper-s3](https://circleci.com/gh/Financial-Times/zipper-s3)

## Service endpoints
_What are the endpoints offered by the service_

e.g.
### GET

Using curl:

    curl http://localhost:8080/people/143ba45c-2fb3-35bc-b227-a6ed80b5c517 | json_pp`

Or using [httpie](https://github.com/jkbrzt/httpie):

    http GET http://localhost:8080/people/143ba45c-2fb3-35bc-b227-a6ed80b5c517

The expected response will contain information about the person, and the organisations they are connected to (via memberships).

Based on the following [google doc](https://docs.google.com/document/d/1SC4Uskl-VD78y0lg5H2Gq56VCmM4OFHofZM-OvpsOFo/edit#heading=h.qjo76xuvpj83).


## Utility endpoints
_Endpoints that are there for support or testing, e.g read endpoints on the writers_

## Healthchecks
Admin endpoints are:

`/__gtg`

`/__health`

`/__build-info`

_These standard endpoints do not need to be specifically documented._

_This section *should* however explain what checks are done to determine health and gtg status._

There are several checks performed:

_e.g._
* Checks that a connection can be made to Neo4j, using the neo4j url supplied as a parameter in service startup.

## Other information
_Anything else you want to add._

_e.g. (NB: this example may be something we want to extract as it's probably common to a lot of services)_

### Logging

* The application uses [logrus](https://github.com/Sirupsen/logrus); the log file is initialised in [main.go](main.go).
* Logging requires an `env` app parameter, for all environments other than `local` logs are written to file.
* When running locally, logs are written to console. If you want to log locally to file, you need to pass in an env parameter that is != `local`.
* NOTE: `/__build-info` and `/__gtg` endpoints are not logged as they are called every second from varnish/vulcand and this information is not needed in logs/splunk.
