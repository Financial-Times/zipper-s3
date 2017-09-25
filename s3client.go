package main

import (
	"fmt"
	"github.com/minio/minio-go"
	"os"
)

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
		return getObjectFromS3(s3Client, bucketName, fileName, noOfRetries-1)
	}

	return obj, nil
}
