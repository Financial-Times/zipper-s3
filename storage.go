package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/minio/minio-go/v6"
	log "github.com/sirupsen/logrus"
)

type s3Config struct {
	client            s3Client
	bucketName        string
	contentFolderName string
	conceptFolderName string
	archivesFolder    string
}

type s3Object interface {
	Stat() (minio.ObjectInfo, error)
	Close() error
	Read(p []byte) (int, error)
}

type s3Client interface {
	GetObject(bucketName, objectName string, opts minio.GetObjectOptions) (*minio.Object, error)
	FPutObject(bucketName, objectName, filePath string, opts minio.PutObjectOptions) (n int64, err error)
	ListObjects(bucketName, objectPrefix string, recursive bool, doneCh <-chan struct{}) <-chan minio.ObjectInfo
}

type s3 interface {
	uploadFile(localZipFileName string, s3ZipName string) error
	downloadFile(fileName string, noOfRetries int) (s3Object, error)
	getFileKeys() ([]string, uint64, error)
}

func newS3Config(s3Client s3Client, bucketName string, contentFolderName string, conceptFolderName string, archivesFolder string) *s3Config {
	return &s3Config{
		client:            s3Client,
		bucketName:        bucketName,
		contentFolderName: contentFolderName,
		conceptFolderName: conceptFolderName,
		archivesFolder:    archivesFolder,
	}
}

func (s3Config *s3Config) uploadFile(localFileName string, s3FileName string) error {
	log.Infof("Uploading file %s to s3...", localFileName)
	_, err := s3Config.client.FPutObject(
		s3Config.bucketName,
		fmt.Sprintf("%s/%s", s3Config.archivesFolder, s3FileName),
		localFileName,
		minio.PutObjectOptions{ContentType: "application/octet-stream"},
	)
	if err != nil {
		return fmt.Errorf("could not upload file with name %s to s3:%w", s3FileName, err)
	}

	log.Infof("Finished uploading file %s to s3", localFileName)
	return nil
}

func (s3Config *s3Config) downloadFile(fileName string, noOfRetries int) (s3Object, error) {
	if noOfRetries == 0 {
		return nil, fmt.Errorf("Cannot download file with name %s from s3", fileName)
	}

	obj, err := s3Config.client.GetObject(s3Config.bucketName, fileName, minio.GetObjectOptions{})
	if err != nil {
		log.WithError(err).Errorf("Cannot download file with name %s from s3. Sleeping for 5 seconds and retrying..", fileName)
		time.Sleep(5 * time.Second)
		return s3Config.downloadFile(fileName, noOfRetries-1)
	}

	return obj, nil
}

func (s3Config *s3Config) getFileKeys(folderName string) ([]string, error) {
	log.Infof("Starting fileKeys retrieval from s3 folder: %s..", folderName)
	doneCh := make(chan struct{})
	s3ListObjectsChannel := s3Config.client.ListObjects(s3Config.bucketName, folderName, true, doneCh)
	fileKeys := make([]string, 0)
	for s3Object := range s3ListObjectsChannel {
		if s3Object.Err != nil {
			return []string{}, fmt.Errorf("Cannot get file info from s3, error was: %s", s3Object.Err)
		}

		fileKey := s3Object.Key

		if isFolder := strings.HasSuffix(fileKey, "/"); isFolder {
			continue
		}

		fileKeys = append(fileKeys, s3Object.Key)
	}

	log.Infof("Finished fileKeys retrieval from s3 folder name %s. There are %d files", folderName, len(fileKeys))
	return fileKeys, nil
}
