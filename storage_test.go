package main

import (
	"bytes"
	"errors"
	"io"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

const (
	validFileName     = "valid-object.json"
	invalidFileName   = "invalid-object.json"
	nonExistingZip    = "non-existing-zip.zip"
	nonExistingBucket = "fake-bucket"

	testzipMD5  = "cnFydJN8FOw0Ry6PbQZ7Sg=="
	testzipPath = "testdata/test.zip"
)

var (
	testFolderFiles = []string{
		"test-folder/file1.txt",
		"test-folder/file2.txt",
		"test-folder/file3.txt",
		"test-folder/file4.txt",
		"test-folder/file5.txt",
		"test-folder/file6.txt",
	}
)

func init() {
	log.SetLevel(log.ErrorLevel)
}

type mockS3Client struct {
	s3iface.S3API
}

func (m *mockS3Client) PutObject(poi *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	if *poi.Bucket == nonExistingBucket {
		return nil, awserr.New("NoSuchBucket", "The specified bucket does not exist", nil)
	}

	if *poi.Key == "test-folder/test.zip" {
		if *poi.ContentMD5 != testzipMD5 {
			return nil, awserr.New("BadDigest", "The Content-MD5 you specified did not match what we received.", nil)
		}
	}

	return nil, nil
}

func (m *mockS3Client) GetObject(goi *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if *goi.Key == validFileName {
		return &s3.GetObjectOutput{
			Body: io.NopCloser(bytes.NewReader([]byte("contents"))),
		}, nil
	}
	if *goi.Key == invalidFileName {
		return nil, awserr.New("NoSuchKey", "The specified key does not exist.", nil)
	}

	return nil, errors.New("error")
}

func (m *mockS3Client) ListObjectsV2(loi *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	if *loi.Bucket == nonExistingBucket {
		return nil, awserr.New("NoSuchBucket", "The specified bucket does not exist", nil)
	}

	// We are ignoring the MaxKeys field in order to test the pagination
	if *loi.Prefix == "test-folder" {
		var isTruncated bool
		var contents = make([]*s3.Object, 0, 4)
		var from, to int

		if *loi.StartAfter == "" {
			isTruncated = true
			from, to = 0, 4
		} else {
			from, to = 4, len(testFolderFiles)
		}

		for i := from; i < to; i++ {
			contents = append(contents, &s3.Object{
				Key: &testFolderFiles[i],
			})
		}

		return &s3.ListObjectsV2Output{
			IsTruncated: aws.Bool(isTruncated),
			Contents:    contents,
		}, nil
	}
	if *loi.Prefix == "empty-folder" {
		return &s3.ListObjectsV2Output{
			IsTruncated: aws.Bool(false),
		}, nil
	}

	return nil, errors.New("error")
}

func TestDownloadFileHappyFlow(t *testing.T) {
	s3Config := newS3Config(&mockS3Client{}, "test-bucket", "")

	downloadedFile, err := s3Config.downloadFile(validFileName, 2)

	assert.Nil(t, err)
	assert.NotNil(t, downloadedFile)
}

func TestDownloadFileWithInvalidFileName(t *testing.T) {
	s3Config := newS3Config(&mockS3Client{}, "test-bucket", "")

	downloadedFile, err := s3Config.downloadFile(invalidFileName, 3)

	assert.NotNil(t, err)
	assert.Nil(t, downloadedFile)
}

func TestUploadFile(t *testing.T) {
	tests := map[string]struct {
		bucketName string
		sourceName string
		expErr     bool
	}{
		"Success": {
			bucketName: "archives",
			sourceName: testzipPath,
		},
		"NoExistingFile": {
			bucketName: "archives",
			sourceName: nonExistingZip,
			expErr:     true,
		},
		"ErrFromS3Client": {
			bucketName: "fake-bucket",
			sourceName: "",
			expErr:     true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			s3Config := newS3Config(&mockS3Client{}, test.bucketName, "test-folder")
			err := s3Config.uploadFile(test.sourceName, "test.zip")

			if err == nil && test.expErr {
				t.Fatalf("expected error, did not get one")
			}
			if err != nil && !test.expErr {
				t.Fatalf("did not expect error, got: %s", err)
			}
		})
	}
}

func TestGetFileKeys(t *testing.T) {
	tests := map[string]struct {
		bucketName string
		folderName string
		want       []string
		expErr     bool
	}{
		"Success": {
			bucketName: "test-bucket",
			folderName: "test-folder",
			want:       testFolderFiles,
		},
		"EmptyFolder": {
			bucketName: "test-bucket",
			folderName: "empty-folder",
		},
		"ErrorFromS3": {
			bucketName: nonExistingBucket,
			folderName: "some-folder",
			expErr:     true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			s3Config := newS3Config(&mockS3Client{}, test.bucketName, "archives")
			got, err := s3Config.getFileKeys(test.folderName)

			if err != nil && !test.expErr {
				t.Fatalf("did not expect error, got: %s", err)
			}
			if err == nil && test.expErr {
				t.Fatalf("expected error, did not get one")
			}

			if !equalSlices(got, test.want) {
				t.Fatalf("want: %v, got: %v", test.want, got)
			}
		})
	}
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
