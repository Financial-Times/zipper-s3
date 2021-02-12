package main

import (
	"archive/zip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	dateFormat          = "2006-01-02"
	fileRemovedS3ErrMsg = "The specified key does not exist."
)

type zipConfig struct {
	zipName        string
	fileSelectorFn fileSelector
	year           int
	fileKeys       []string
}

type fileSelector func(year int, s3ObjectKey string) (bool, error)

func newZipConfig(zipName string, fileSelectorFn fileSelector, year int, fileKeys []string) *zipConfig {
	return &zipConfig{
		zipName:        zipName,
		fileSelectorFn: fileSelectorFn,
		year:           year,
		fileKeys:       fileKeys,
	}
}

func zipAndUploadFiles(s3Config *s3Config, zipConfig *zipConfig, done chan bool, errsCh chan error) {
	defer func() {
		done <- true
	}()

	tempZipFileName, noOfZippedFiles, err := createZipFiles(s3Config, zipConfig)
	defer os.Remove(tempZipFileName)

	if err != nil {
		errsCh <- fmt.Errorf("Zip creation failed for zip with name %s. Error was: %s", zipConfig.zipName, err)
		return
	}

	if noOfZippedFiles == 0 {
		log.Warnf("There is no content file on S3 to be added to archive with name %s. The s3 file prefix that has been used is %s", zipConfig.zipName, s3Config.contentFolderName)
		return
	}

	/* 	//upload zip file to s3
	   	err = s3Config.uploadFile(tempZipFileName, zipConfig.zipName)
	   	if err != nil {
	   		errsCh <- fmt.Errorf("Cannot upload zip with name %s to S3. Error was: %s", tempZipFileName, err)
	   	}

	   	runtime.GC() */
}

func createZipFiles(s3Config *s3Config, zipConfig *zipConfig) (string, int, error) {
	log.Infof("Starting zip creation process for archive with name %s", zipConfig.zipName)
	startTime := time.Now()

	doneCh := make(chan struct{})
	defer close(doneCh)

	zipFile, err := ioutil.TempFile(os.TempDir(), zipConfig.zipName)
	if err != nil {
		return "", 0, fmt.Errorf("Cannot create archive: %s", err)
	}
	defer zipFile.Close()

	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()
	log.Infof("Starting to zip files into archive with name %s", zipConfig.zipName)
	noOfZippedFiles := 0

	for _, s3ObjectKey := range zipConfig.fileKeys {
		if zipConfig.fileSelectorFn != nil {
			isEligible, err := zipConfig.fileSelectorFn(zipConfig.year, s3ObjectKey)
			if err != nil {
				log.WithError(err).Errorf("Cannot select S3 object with key %s.", s3ObjectKey)
				continue
			}

			if !isEligible {
				continue
			}
		}

		noOfZippedFiles++

		s3File, err := s3Config.downloadFile(s3ObjectKey, 3)
		if err != nil {
			return "", 0, fmt.Errorf("Cannot download file with name %s from s3: %s", s3ObjectKey, err)
		}

		fileInfo, err := s3File.Stat()
		if err != nil {
			if err.Error() == fileRemovedS3ErrMsg {
				log.Infof("File with name %s was deleted since the zip up process started for zip %s", s3ObjectKey, zipConfig.zipName)
				continue
			}

			return "", 0, fmt.Errorf("Cannot download file with name %s from s3: %s", s3ObjectKey, err)
		}

		//add file to zip
		fileNameSplit := strings.Split(fileInfo.Key, "/")
		fileName := fileInfo.Key
		if len(fileNameSplit) > 0 {
			fileName = fileNameSplit[len(fileNameSplit)-1]
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
	log.Infof("Finished zip creation process for zip with name %s. Duration: %s. Number of zipped files is: %d", zipConfig.zipName, zippingUpDuration, noOfZippedFiles)
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

	fileName := strings.TrimSuffix(s3ObjectKeySplit[len(s3ObjectKeySplit)-1], ".json")
	fileNameSplit := strings.Split(fileName, "_")

	if len(fileNameSplit) < 2 {
		return time.Now(), fmt.Errorf("Cannot extract date from file name: %s", fileName)
	}

	dateString := fileNameSplit[len(fileNameSplit)-1]

	date, err := time.Parse(dateFormat, dateString)
	if err != nil {
		return time.Now(), fmt.Errorf("Cannot parse date from s3 file key %s, error was: %s", s3ObjectKey, err)
	}

	return date, nil
}

func groupS3ObjectsByYear(objects []string) map[int][]string {
	groupedObjects := make(map[int][]string)
	for _, obj := range objects {
		s3ObjectDate, err := extractDateFromS3ObjectKey(obj)
		if err != nil {
			log.WithError(err).Errorf("Cannot extract date from file name %s, error was: %s", obj, err)
			continue
		}

		if len(groupedObjects[s3ObjectDate.Year()]) > 0 {
			continue
		}
		groupedObjects[s3ObjectDate.Year()] = append(groupedObjects[s3ObjectDate.Year()], obj)

		if isDateLessThanThirtyDaysBefore(s3ObjectDate) {
			if len(groupedObjects[0]) > 0 {
				continue
			}
			groupedObjects[0] = append(groupedObjects[0], obj)
		}
	}

	return groupedObjects
}
