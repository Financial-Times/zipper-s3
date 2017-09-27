package main

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

const contentUUID = "00544bc0-679f-11e7-9d4e-ae21227e5abf.json"

func TestIsDateLessThanThirtyDaysBeforeOneHourBefore(t *testing.T) {
	currentDate := time.Now()
	previousDate := currentDate.Add(-1 * time.Hour)

	dateLessThanThirtyDaysBefore := isDateLessThanThirtyDaysBefore(previousDate)

	assert.True(t, dateLessThanThirtyDaysBefore)
}

func TestIsDateLessThanThirtyDaysBeforeFortyDaysBefore(t *testing.T) {
	currentDate := time.Now()
	previousDate := currentDate.Add(-40 * 24 * time.Hour)

	dateLessThanThirtyDaysBefore := isDateLessThanThirtyDaysBefore(previousDate)

	assert.False(t, dateLessThanThirtyDaysBefore)
}

func TestIsContentLessThanThirtyDaysBefore(t *testing.T) {
	currentDate := time.Now()
	previousDay := currentDate.Add(-24 * time.Hour)
	previousDayString := previousDay.Format("2006-01-02")
	fileNameLessThanThirtyDaysBefore := fmt.Sprintf("%s%s", previousDayString, contentUUID)

	contentLessThanThirtyDaysBefore, err := isContentLessThanThirtyDaysBefore(fileNameLessThanThirtyDaysBefore)

	assert.Nil(t, err)
	assert.True(t, contentLessThanThirtyDaysBefore)
}

func TestIsContentMoreThanThirtyDaysBefore(t *testing.T) {
	currentDate := time.Now()
	previousDay := currentDate.Add(-24 * 40 * time.Hour)
	previousDayString := previousDay.Format("2006-01-02")
	fileNameLessThanThirtyDaysBefore := fmt.Sprintf("%s%s", previousDayString, contentUUID)

	contentLessThanThirtyDaysBefore, err := isContentLessThanThirtyDaysBefore(fileNameLessThanThirtyDaysBefore)

	assert.Nil(t, err)
	assert.False(t, contentLessThanThirtyDaysBefore)
}

func TestIsContentLessThanThirtyDaysBeforeInvalidFileName(t *testing.T) {
	_, err := isContentLessThanThirtyDaysBefore(contentUUID)

	assert.NotNil(t, err)
}

func TestIsContentMoreThanThirtyDaysBeforeInvalidDateFormat(t *testing.T) {
	currentDate := time.Now()
	previousDay := currentDate.Add(-24 * 40 * time.Hour)
	previousDayString := previousDay.Format("01-02-2016")
	fileNameLessThanThirtyDaysBefore := fmt.Sprintf("%s%s", previousDayString, contentUUID)

	_, err := isContentLessThanThirtyDaysBefore(fileNameLessThanThirtyDaysBefore)

	assert.NotNil(t, err)
}

func TestZipFilesNoFiles(t *testing.T) {
	initLogs(os.Stdout, os.Stdout, os.Stderr)
	s3Config := newS3Config(&mockS3Client{}, "test-bucket")

	_, noOfZippedFiles, err := zipFiles(s3Config, "", "", nil)

	assert.Nil(t, err)
	assert.Zero(t, noOfZippedFiles)
}
