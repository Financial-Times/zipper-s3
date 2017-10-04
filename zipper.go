package main

import (
	"archive/zip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"time"
)

const (
	dateFormat = "2006-01-02"
	fileRemovedS3ErrmSG = "The specified key does not exist."
)

var dateRegexp = regexp.MustCompile(`(19|20)\d\d-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])`)

type fileSelector func(year int, s3ObjectKey string) (bool, error)

func zipAndUploadFiles(s3Config *s3Config, s3ObjectKeyPrefix string, zipName string, fileSelectorFn fileSelector, done chan bool, errsCh chan error, year int) {
	defer func() {
		done <- true
	}()

	tempZipFileName, noOfZippedFiles, err := zipFiles(s3Config, s3ObjectKeyPrefix, zipName, fileSelectorFn, year)
	defer os.Remove(tempZipFileName)

	if err != nil {
		errsCh <- fmt.Errorf("Zip creation failed for zip with name %s. Error was: %s", zipName, err)
		return
	}

	if noOfZippedFiles == 0 {
		warnLogger.Printf("There is no content file on S3 to be added to archive with name %s. The s3 file prefix that has been used is %s", zipName, s3ObjectKeyPrefix)
		return
	}

	//upload zip file to s3
	err = s3Config.uploadFile(tempZipFileName, zipName)
	if err != nil {
		errsCh <- fmt.Errorf("Cannot upload file to S3. Error was: %s", err)
	}

	return
}

func zipFiles(s3Config *s3Config, s3ObjectKeyPrefix string, zipName string, fileSelectorFn fileSelector, year int) (string, int, error) {
	infoLogger.Printf("Starting zip creation process for archive with name %s", zipName)
	startTime := time.Now()

	doneCh := make(chan struct{})
	defer close(doneCh)

	zipFile, err := ioutil.TempFile(os.TempDir(), zipName)
	defer zipFile.Close()
	if err != nil {
		return "", 0, fmt.Errorf("Cannot create archive: %s", err)
	}
	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()
	infoLogger.Printf("Starting to zip files into archive with name %s", zipName)
	noOfZippedFiles := 0
	s3ListObjectsChannel := s3Config.client.ListObjects(s3Config.bucketName, s3ObjectKeyPrefix, true, doneCh)

	for s3Object := range s3ListObjectsChannel {
		if s3Object.Err != nil {
			return "", 0, fmt.Errorf("Error while receiving objectInfo: %s", s3Object.Err)
		}

		if fileSelectorFn != nil {
			selectFile, err := fileSelectorFn(year, s3Object.Key)
			if err != nil {
				errorLogger.Printf("Cannot select S3 object with key %s. Error was: %s", s3Object.Key, err)
				continue
			}

			if !selectFile {
				continue
			}
		}

		noOfZippedFiles++

		s3File, err := s3Config.downloadFile(s3Object.Key, 3)
		if err != nil {
			return "", 0, fmt.Errorf("Cannot download file with name %s from s3: %s", s3Object.Key, err)
		}

		fileInfo, err := s3File.Stat()
		if err != nil {
			if err.Error() == fileRemovedS3ErrmSG {
				infoLogger.Printf("File with name %s was deleted since the zip up process started for zip %s", s3Object.Key, zipName)
				continue
			}

			return "", 0, fmt.Errorf("Cannot download file with name %s from s3: %s", s3Object.Key, err)
		}

		//add file to zip
		fileNameSplit := strings.Split(fileInfo.Key, "/")
		fileName := fileInfo.Key
		if len(fileNameSplit) > 0 {
			fileName = fileNameSplit[len(fileNameSplit) - 1]
		}

		h := &zip.FileHeader{
			Name:   fileName,
			Method: zip.Deflate,
			Flags:  0x800,
		}
		f, err := zipWriter.CreateHeader(h)
		if err != nil {
			return "", 0, fmt.Errorf("Cannot create zip header for file, error was: %s", err)
		}

		_, err = io.Copy(f, s3File)
		if err != nil {
			return "", 0, fmt.Errorf("Cannot add file to zip archive: %s", err)
		}

		s3File.Close()
	}

	zippingUpDuration := time.Since(startTime)
	infoLogger.Printf("Finished zip creation process for zip with name %s. Duration: %s. Number of zipped files is: %d", zipName, zippingUpDuration, noOfZippedFiles)
	return zipFile.Name(), noOfZippedFiles, nil
}

func isDateLessThanThirtyDaysBefore(date time.Time) bool {
	thirtyDays := time.Duration(30 * 24 * time.Hour)
	return time.Since(date) < thirtyDays
}

func isContentFromProvidedYear(year int, s3ObjectKey string) (bool, error) {
	s3ObjectDate, err := extractDateFromS3ObjectKey(s3ObjectKey)
	if err != nil {
		return false, fmt.Errorf("Cannot extract date from file name %s, error was: %s", s3ObjectKey, err)
	}

	if year == s3ObjectDate.Year() {
		return true, nil
	}

	return false, nil
}

func isContentLessThanThirtyDaysBefore(year int, s3ObjectKey string) (bool, error) {
	s3ObjectDate, err := extractDateFromS3ObjectKey(s3ObjectKey)
	if err != nil {
		return false, fmt.Errorf("Cannot extract date from file name %s, error was: %s", s3ObjectKey, err)
	}

	return isDateLessThanThirtyDaysBefore(s3ObjectDate), nil
}

func extractDateFromS3ObjectKey(s3ObjectKey string) (time.Time, error) {
	s3ObjectKeySplit := strings.Split(s3ObjectKey, "/")
	if len(s3ObjectKeySplit) < 1 {
		return time.Now(), fmt.Errorf("Cannot extract date from s3Objectkey: %s", s3ObjectKey)
	}

	fileName := strings.TrimSuffix(s3ObjectKeySplit[len(s3ObjectKeySplit) - 1], ".json")
	fileNameSplit := strings.Split(fileName, "_")

	if len(fileNameSplit) < 2 {
		return time.Now(), fmt.Errorf("Cannot extract date from file name: %s", fileName)
	}

	dateString := fileNameSplit[len(fileNameSplit) - 1]

	date, err := time.Parse(dateFormat, dateString)
	if err != nil {
		return time.Now(), fmt.Errorf("Cannot parse date from s3 file key %s, error was: %s", s3ObjectKey, err)
	}

	return date, nil
}