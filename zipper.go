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

const dateFormat = "2006-01-02"

var dateRegexp = regexp.MustCompile(`(19|20)\d\d-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])`)

type fileSelector func(s3ObjectKey string) (bool, error)

func zipAndUploadFiles(s3Config *s3Config, s3ObjectKeyPrefix string, zipName string, fileSelectorFn fileSelector, done chan bool, errsCh chan error) {
	defer func() {
		done <- true
	}()

	tempZipFileName, noOfZippedFiles, err := zipFiles(s3Config, s3ObjectKeyPrefix, zipName, fileSelectorFn)
	defer os.Remove(tempZipFileName)

	if err != nil {
		errsCh <- err
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

func zipFiles(s3Config *s3Config, s3ObjectKeyPrefix string, zipName string, fileSelectorFn fileSelector) (string, int, error) {
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
			selectFile, err := fileSelectorFn(s3Object.Key)
			if err != nil {
				errorLogger.Printf("Cannot select S3 object. Error was: %s", err)
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

		//add file to zip
		fileInfo, err := s3File.Stat()
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
	}

	zippingUpDuration := time.Since(startTime)
	infoLogger.Printf("Finished zip creation process for zip with name %s. Duration: %s. Number of zipped files is: %d", zipName, zippingUpDuration, noOfZippedFiles)
	return zipFile.Name(), noOfZippedFiles, nil
}

func isDateLessThanThirtyDaysBefore(date time.Time) bool {
	thirtyDays := time.Duration(30 * 24 * time.Hour)
	return time.Since(date) < thirtyDays
}

func isContentLessThanThirtyDaysBefore(s3ObjectKey string) (bool, error) {
	//check if the date is less that thirty days ago.
	match := dateRegexp.FindStringSubmatch(s3ObjectKey)
	if len(match) < 1 {
		return false, fmt.Errorf("Cannot parse date from s3 file name: %s", s3ObjectKey)
	}

	s3FileDate, err := time.Parse(dateFormat, match[0])
	if err != nil {
		return false, fmt.Errorf("Cannot parse date from s3 file name, error was: %s", err)
	}

	return isDateLessThanThirtyDaysBefore(s3FileDate), nil
}
