package main

import (
	"fmt"
	"testing"
	"time"

	minio "github.com/minio/minio-go/v6"
	"github.com/stretchr/testify/assert"
)

const contentUUID = "00544bc0-679f-11e7-9d4e-ae21227e5abf"

type s3ObjectMock struct {
	name    string
	content string
}

func (s *s3ObjectMock) Stat() (minio.ObjectInfo, error) {
	return minio.ObjectInfo{Key: s.name}, nil
}

func (s *s3ObjectMock) Close() error {
	return nil

}

func (s *s3ObjectMock) Read(p []byte) (int, error) {
	s.content = string(p)
	return 0, nil
}

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
	previousDayString := previousDay.Format(dateFormat)
	fileNameLessThanThirtyDaysBefore := fmt.Sprintf("test/%s_%s.json", contentUUID, previousDayString)

	contentLessThanThirtyDaysBefore, err := isContentLessThanThirtyDaysBefore(0, fileNameLessThanThirtyDaysBefore)

	assert.Nil(t, err)
	assert.True(t, contentLessThanThirtyDaysBefore)
}

func TestIsContentMoreThanThirtyDaysBefore(t *testing.T) {
	currentDate := time.Now()
	previousDay := currentDate.Add(-24 * 40 * time.Hour)
	previousDayString := previousDay.Format(dateFormat)
	fileNameLessThanThirtyDaysBefore := fmt.Sprintf("test/%s_%s.json", contentUUID, previousDayString)

	contentLessThanThirtyDaysBefore, err := isContentLessThanThirtyDaysBefore(0, fileNameLessThanThirtyDaysBefore)

	assert.Nil(t, err)
	assert.False(t, contentLessThanThirtyDaysBefore)
}

func TestIsContentLessThanThirtyDaysBeforeInvalidFileName(t *testing.T) {
	_, err := isContentLessThanThirtyDaysBefore(0, contentUUID)

	assert.NotNil(t, err)
}

func TestIsContentMoreThanThirtyDaysBeforeInvalidDateFormat(t *testing.T) {
	currentDate := time.Now()
	previousDay := currentDate.Add(-24 * 40 * time.Hour)
	previousDayString := previousDay.Format(dateFormat)
	fileNameLessThanThirtyDaysBefore := fmt.Sprintf("%s%s", previousDayString, contentUUID)

	_, err := isContentLessThanThirtyDaysBefore(0, fileNameLessThanThirtyDaysBefore)

	assert.NotNil(t, err)
}

func TestZipFilesNoFiles(t *testing.T) {
	s3Config := newS3Config(&mockS3Client{}, "test-bucket", "", "", "")
	zipConfig := newZipConfig("", nil, 0, []string{})

	_, noOfZippedFiles, err := createZipFiles(s3Config, zipConfig)

	assert.Nil(t, err)
	assert.Zero(t, noOfZippedFiles)
}

func TestExtractDateFromS3ObjectKeyValidObjectKey(t *testing.T) {
	s3ObjectKey := fmt.Sprintf("%s_2016-10-30.json", contentUUID)

	date, err := extractDateFromS3ObjectKey(s3ObjectKey)

	assert.Nil(t, err)
	assert.Equal(t, 2016, date.Year())
	assert.Equal(t, time.October, date.Month())
	assert.Equal(t, 30, date.Day())
}

func TestExtractDateFromS3ObjectKeyMissingDate(t *testing.T) {
	s3ObjectKey := fmt.Sprintf("%s.json", contentUUID)

	_, err := extractDateFromS3ObjectKey(s3ObjectKey)

	assert.NotNil(t, err)
}

func TestExtractDateFromS3ObjectKeyMissingUUID(t *testing.T) {
	s3ObjectKey := "test/2016-10-20.json"

	_, err := extractDateFromS3ObjectKey(s3ObjectKey)

	assert.NotNil(t, err)
}

func TestExtractDateFromS3ObjectKeyInvalidDateFormat(t *testing.T) {
	s3ObjectKey := fmt.Sprintf("test/%s_20-10-2015.json", contentUUID)

	_, err := extractDateFromS3ObjectKey(s3ObjectKey)

	assert.NotNil(t, err)
}

func TestIsContentFromProvidedYearProvidedYearIsTheSame(t *testing.T) {
	s3ObjectKey := fmt.Sprintf("%s_2016-10-30.json", contentUUID)

	isContentFromProvidedYearFlag, err := isContentFromProvidedYear(2016, s3ObjectKey)

	assert.Nil(t, err)
	assert.True(t, isContentFromProvidedYearFlag)
}

func TestIsContentFromProvidedYearProvidedYearIsDifferent(t *testing.T) {
	s3ObjectKey := fmt.Sprintf("%s_2016-10-30.json", contentUUID)

	isContentFromProvidedYearFlag, err := isContentFromProvidedYear(2017, s3ObjectKey)

	assert.Nil(t, err)
	assert.False(t, isContentFromProvidedYearFlag)
}

func TestIsContentFromProvidedYearProvidedKeyIsInvalid(t *testing.T) {
	s3ObjectKey := fmt.Sprintf("%s.json", contentUUID)

	_, err := isContentFromProvidedYear(2017, s3ObjectKey)

	assert.NotNil(t, err)
}

func TestZipFilesInvalidFileName(t *testing.T) {
	s3Config := newS3Config(&mockS3Client{}, "test-bucket", "", "", "")
	zipConfig := newZipConfig("yearly-archive-2017.zip", nil, 2017, []string{"invalid-file"})

	_, _, err := createZipFiles(s3Config, zipConfig)

	assert.NotNil(t, err)
}
