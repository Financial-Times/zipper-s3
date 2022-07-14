package main

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go/aws"
	s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type s3Config struct {
	svc            s3iface.S3API
	bucketName     string
	archivesFolder string
}

func newS3Config(s3Client s3iface.S3API, bucketName, archivesFolder string) *s3Config {
	return &s3Config{
		svc:            s3Client,
		bucketName:     bucketName,
		archivesFolder: archivesFolder,
	}
}

func (s3Config *s3Config) uploadFile(localFileName string, s3FileName string) error {
	log.Infof("Uploading file %s to s3...", localFileName)

	f, err := os.ReadFile(localFileName)
	if err != nil {
		return fmt.Errorf("reading file: %w", err)
	}

	fileHash := md5.Sum([]byte(f))
	// EncodeToString want slice, not array
	base64EncodedMD5Hash := base64.StdEncoding.EncodeToString(fileHash[:])

	input := &s3.PutObjectInput{
		Bucket: aws.String(s3Config.bucketName),
		Key:    aws.String(fmt.Sprintf("%s/%s", s3Config.archivesFolder, s3FileName)),
		Body:   bytes.NewReader(f),

		// Optional: integrity check to verify that the data is the same data
		// that was originally sent.
		ContentMD5: aws.String(base64EncodedMD5Hash),
	}

	_, err = s3Config.svc.PutObject(input)
	if err != nil {
		return fmt.Errorf("could not upload file with name %s to s3:%w", s3FileName, err)
	}

	log.Infof("Finished uploading file %s to s3", localFileName)
	return nil
}

// TODO: aws sdk supports retrying mechanism
func (s3Config *s3Config) downloadFile(fileName string, noOfRetries int) (s3Object, error) {
	input := &s3.GetObjectInput{
		Bucket: aws.String(s3Config.bucketName),
		Key:    aws.String(fileName),
	}
	output, err := s3Config.svc.GetObject(input)
	if err != nil {
		log.WithError(err).Errorf("Cannot download file with name %s from s3. Sleeping for 5 seconds and retrying..", fileName)
		time.Sleep(5 * time.Second)

		// check here so that we can wrap the original error returned from AWS
		if noOfRetries < 1 {
			return nil, fmt.Errorf("downloading file: %w", err)
		}
		return s3Config.downloadFile(fileName, noOfRetries-1)
	}

	return &s3obj{
		key:  fileName,
		data: output.Body,
	}, nil
}

func (s3Config *s3Config) getFileKeys(folderName string) ([]string, error) {
	log.Infof("Starting fileKeys retrieval from s3 folder: %s..", folderName)

	listObjects := func(startAfter string) ([]string, bool, error) {
		input := &s3.ListObjectsV2Input{
			Bucket:     aws.String(s3Config.bucketName),
			Prefix:     aws.String(folderName),
			StartAfter: aws.String(startAfter),
		}
		output, err := s3Config.svc.ListObjectsV2(input)
		if err != nil {
			return nil, false, fmt.Errorf("listing objects: %w", err)
		}

		keys := make([]string, 0, len(output.Contents))
		for _, obj := range output.Contents {
			keys = append(keys, *obj.Key)
		}

		return keys, *output.IsTruncated, nil
	}

	result := make([]string, 0, 32)
	lastKey := ""
	for {
		keys, more, err := listObjects(lastKey)
		if err != nil {
			return nil, err
		}
		result = append(result, keys...)
		if !more {
			break
		}
		lastKey = keys[len(keys)-1]
	}

	log.Infof("Finished fileKeys retrieval from s3 folder name %s. There are %d files", folderName, len(result))
	return result, nil
}

type s3Object interface {
	Key() string
	Close() error
	Read(p []byte) (int, error)
}

type s3obj struct {
	data io.ReadCloser
	key  string
}

func (o *s3obj) Key() string {
	return o.key
}

func (o *s3obj) Close() error {
	return o.data.Close()
}

func (o *s3obj) Read(p []byte) (int, error) {
	return o.data.Read(p)
}
