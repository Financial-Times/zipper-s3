package main

import (
	"fmt"
	"github.com/minio/minio-go"
	"io"
	"os"
	"time"
)

type s3Config struct {
	client          s3Client
	bucketName      string
	objectkeyPrefix string
}

type s3Client interface {
	GetObject(bucketName, objectName string) (*minio.Object, error)
	PutObject(bucketName, objectName string, reader io.Reader, contentType string) (n int64, err error)
	ListObjects(bucketName, objectPrefix string, recursive bool, doneCh <-chan struct{}) <-chan minio.ObjectInfo
}

type s3 interface {
	uploadFile(localZipFileName string, s3ZipName string) error
	downloadFile(fileName string, noOfRetries int) (*minio.Object, error)
	getFileKeys() ([]string, uint64, error)
}

func newS3Config(s3Client s3Client, bucketName string, objectKeyPrefix string) *s3Config {
	return &s3Config{
		client:     s3Client,
		bucketName: bucketName,
		objectkeyPrefix: objectKeyPrefix,
	}
}

func (s3Config *s3Config) uploadFile(localFileName string, s3FileName string) error {
	infoLogger.Printf("Uploading file %s to s3...", localFileName)
	zipFileToBeUploaded, err := os.Open(localFileName)
	if err != nil {
		return fmt.Errorf("Could not open zip archive with name %s. Error was: %s", s3FileName, err)
	}
	defer zipFileToBeUploaded.Close()

	_, err = s3Config.client.PutObject(s3Config.bucketName, fmt.Sprintf("yearly-archives/%s", s3FileName), zipFileToBeUploaded, "application/octet-stream")
	if err != nil {
		return fmt.Errorf("Could not upload file with name %s to s3. Error was: %s", s3FileName, err)
	}

	infoLogger.Printf("Finished uploading file %s to s3", localFileName)
	return nil
}

func (s3Config *s3Config) downloadFile(fileName string, noOfRetries int) (*minio.Object, error) {
	if noOfRetries == 0 {
		return nil, fmt.Errorf("Cannot download file with name %s from s3.", fileName)
	}

	obj, err := s3Config.client.GetObject(s3Config.bucketName, fileName)
	if err != nil {
		errorLogger.Printf("Cannot download file with name %s from s3. Error was: %s. Sleeping for 5 seconds and retrying..", fileName, err)
		time.Sleep(5 * time.Second)
		return s3Config.downloadFile(fileName, noOfRetries - 1)
	}

	return obj, nil
}

func (s3Config *s3Config) getFileKeys() ([]string, error) {
	infoLogger.Print("Starting fileKeys retrieval from s3..")
	doneCh := make(chan struct{})
	s3ListObjectsChannel := s3Config.client.ListObjects(s3Config.bucketName, s3Config.objectkeyPrefix, true, doneCh)
	fileKeys := make([]string, 0)
	for s3Object := range s3ListObjectsChannel {
		if s3Object.Err != nil {
			return []string{}, fmt.Errorf("Cannot get file info from s3, error was: %s", s3Object.Err)
		}

		fileKeys = append(fileKeys, s3Object.Key)
	}

	infoLogger.Printf("Finished fileKeys retrieval from s3. There are %d files", len(fileKeys))
	return fileKeys, nil
}