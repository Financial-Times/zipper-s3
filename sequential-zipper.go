package main

import (
	"github.com/minio/minio-go"
	"time"
	"os"
	"fmt"
	"archive/zip"
	"io/ioutil"
	"strings"
	"io"
	"sync"
)

func zipAndUploadFilesSequentially(s3Client *minio.Client, bucketName string, s3ObjectKeyPrefix string, zipName string, fileSelectorFn fileSelector, zipperWg *sync.WaitGroup, errsCh chan error) {
	defer zipperWg.Done()
	tempZipFileName, noOfZippedFiles, err := zipFilesSequentially(s3Client, bucketName, s3ObjectKeyPrefix, zipName, fileSelectorFn)

	if noOfZippedFiles == 0 {
		warnLogger.Printf("There is no content file on S3 to be added to archive with name %s. The s3 file prefix that has been used is %s", zipName, s3ObjectKeyPrefix)
		return
	}

	if err != nil {
		errsCh <- err
		return
	}

	//upload zip file to s3
	err = uploadFileToS3(s3Client, bucketName, tempZipFileName, zipName)
	if err != nil {
		errsCh <- fmt.Errorf("Cannot upload file to S3. Error was: %s", err)
	}

	//todo: remove the temp archive.
	return
}

func zipFilesSequentially(s3Client *minio.Client, bucketName string, s3ObjectKeyPrefix string, zipName string, fileSelectorFn fileSelector) (string, int, error) {
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
	s3ListObjectsChannel := s3Client.ListObjects(bucketName, s3ObjectKeyPrefix, true, doneCh)

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

		s3File, err := getObjectFromS3(s3Client, bucketName, s3Object.Key, 3)
		if err != nil {
			return "", 0, fmt.Errorf("Cannot download file with name %s from s3: %s", s3Object.Key, err)
		}

		//infoLogger.Printf("Downloaded file: %s", s3Object.Key)


		//add file to zip
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
			return "", 0, fmt.Errorf("Cannot create zip header for file, error was: %s", err)
		}

		_, err = io.Copy(f, s3File)
		if err != nil {
			return "", 0, fmt.Errorf("Cannot add file to zip archive: %s", err)
		}

		//infoLogger.Printf("Added file with name %s to archive.", fileNameSplit)
	}

	zippingUpDuration := time.Since(startTime)
	infoLogger.Printf("Finished zip creation process for zip with name %s. Duration: %s", zipName, zippingUpDuration)
	return zipFile.Name(), noOfZippedFiles, nil
}
