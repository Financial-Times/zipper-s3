package main

import (
	health "github.com/Financial-Times/go-fthealth/v1_1"
	"github.com/Financial-Times/service-status-go/gtg"
)

const healthPath = "/__health"

type healthService struct {
	config *healthConfig
	checks []health.Check
}

type healthConfig struct {
	appSystemCode string
	appName       string
	port          string
}

func newHealthService(config *healthConfig) *healthService {
	service := &healthService{config: config}
	service.checks = []health.Check{
		service.sampleCheck(),
	}
	return service
}

func (service *healthService) sampleCheck() health.Check {
	return health.Check{
		BusinessImpact:   "Sample healthcheck has no impact",
		Name:             "Sample healthcheck",
		PanicGuide:       "https://dewey.ft.com/zipper-s3.html",
		Severity:         1,
		TechnicalSummary: "Sample healthcheck has no technical details",
		Checker:          service.sampleChecker,
	}
}

func (service *healthService) sampleChecker() (string, error) {
	return "Sample is healthy", nil

}

func (service *healthService) gtgCheck() gtg.Status {
	for _, check := range service.checks {
		if _, err := check.Checker(); err != nil {
			return gtg.Status{GoodToGo: false, Message: err.Error()}
		}
	}
	return gtg.Status{GoodToGo: true}
}
