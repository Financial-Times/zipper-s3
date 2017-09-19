package main

import (
	"fmt"
	"github.com/minio/minio-go"
	"io"
	"os"
	"regexp"
	"sync"
	"time"
	"archive/zip"
	"strings"
	"io/ioutil"
)

const dateFormat = "2006-01-02"

var dateRegexp = regexp.MustCompile(`(19|20)\d\d-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])`)

type fileSelector func(s3ObjectKey string) (bool, error)

func writeZipFile(s3FilesChannel chan *minio.Object, zipName string) (string, *sync.WaitGroup) {
	zipFile, err := ioutil.TempFile(os.TempDir(), zipName)
	if err != nil {
		errorLogger.Printf("Cannot create archive: %s", err)
	}

	var zipWriterWg sync.WaitGroup
	zipWriterWg.Add(1)
	go func() {
		noOfZippedFiles := 0
		defer zipWriterWg.Done()
		infoLogger.Printf("Starting to zip files into archive with name %s", zipName)
		defer zipFile.Close()

		zipWriter := zip.NewWriter(zipFile)
		defer zipWriter.Close()
		for s3File := range s3FilesChannel {
			fileInfo, err := s3File.Stat()
			fileNameSplit := strings.Split(fileInfo.Key, "/")
			fileName := fileInfo.Key
			if len(fileNameSplit) > 0 {
				fileName = fileNameSplit[len(fileNameSplit) - 1]
			}

			h := &zip.FileHeader{
				Name:fileName,
				Method: zip.Deflate,
				Flags:  0x800,
			}
			f, err := zipWriter.CreateHeader(h)
			if err != nil {
				errorLogger.Printf("Cannot create zip header for file, error was: %s", err)
				return
			}

			_, err = io.Copy(f, s3File)
			if err != nil {
				errorLogger.Printf("Cannot add file to zip archive: %s", err)
				return
			}

			infoLogger.Printf("Added file with name %s to archive.", fileNameSplit)

			noOfZippedFiles++
		}

		if noOfZippedFiles == 0 {
			errorLogger.Printf("There are no files added to archive with name %s", zipName)
			return
		}

		infoLogger.Printf("Finished adding files to zip with name %s. Number of zipped files is: %d", zipName, noOfZippedFiles)
	}()

	return zipFile.Name(), &zipWriterWg
}

func zipFilesInParallel(s3Client *minio.Client, bucketName string, s3ObjectKeyPrefix string, zipName string, fileSelectorFn fileSelector) error {
	infoLogger.Printf("Starting parallel zip creation process for archive with name %s", zipName)
	startTime := time.Now()

	doneCh := make(chan struct{})
	defer close(doneCh)

	s3Files := make(chan *minio.Object)
	tempZipFileName, zipWriterWg := writeZipFile(s3Files, zipName)
	defer os.Remove(tempZipFileName)

	var s3DownloadWg sync.WaitGroup
	s3ListObjectsChannel := s3Client.ListObjects(bucketName, s3ObjectKeyPrefix, true, doneCh)
	for s3Object := range s3ListObjectsChannel {
		if s3Object.Err != nil {
			return fmt.Errorf("Error while receiving objectInfo: %s", s3Object.Err)
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

		s3DownloadWg.Add(1)
		go func(s3FileName string) {
			defer s3DownloadWg.Done()

			obj, err := getObjectFromS3(s3Client, bucketName, s3FileName, 3)
			if err != nil {
				errorLogger.Printf("Cannot download file with name %s from s3: %s", s3FileName, err)
				return
			}

			infoLogger.Printf("Downloaded file: %s", s3FileName)

			s3Files <- obj
		}(s3Object.Key)
	}

	s3DownloadWg.Wait()
	close(s3Files)

	zipWriterWg.Wait()
	zippingUpDuration := time.Since(startTime)
	infoLogger.Printf("Finished zip creation process for zip with name %s. Duration: %s", zipName, zippingUpDuration)

	//upload zip file to s3
	err := uploadFileToS3(s3Client, bucketName, tempZipFileName, zipName)
	if err != nil {
		return fmt.Errorf("Cannot upload file to S3. Error was: %s", err)
	}

	return nil
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

func uploadFileToS3(s3Client *minio.Client, bucketName string, localZipFileName string, s3ZipName string) error {

	infoLogger.Printf("Uploading file %s to s3...", localZipFileName)
	zipFileToBeUploaded, err := os.Open(localZipFileName)
	if err != nil {
		return fmt.Errorf("Could not open zip archive with name %s. Error was: %s", s3ZipName, err)
	}
	defer zipFileToBeUploaded.Close()

	_, err = s3Client.PutObject(bucketName, fmt.Sprintf("yearly-archives/%s", s3ZipName), zipFileToBeUploaded, "application/octet-stream")
	if err != nil {
		fmt.Errorf("Could not upload file with name %s to s3. Error was: %s", s3ZipName, err)
	}

	infoLogger.Printf("Finished uploading file %s to s3", localZipFileName)
	return nil
}

func getObjectFromS3(s3Client *minio.Client, bucketName string, fileName string, noOfRetries int) (*minio.Object, error) {
	if noOfRetries == 0 {
		return nil, fmt.Errorf("Cannot download file with name %s from s3.", fileName)
	}

	obj, err := s3Client.GetObject(bucketName, fileName)
	if err != nil {
		errorLogger.Printf("Cannot download file with name %s from s3: %s. Retrying..", fileName, err)
		return getObjectFromS3(s3Client, bucketName, fileName, noOfRetries - 1)
	}

	return obj, nil
}