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
)

const dateTemplate = "2006-01-02"

var dateRegexp = regexp.MustCompile(`(19|20)\d\d-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])`)

func writeZipFile(s3FilesChannel chan *minio.Object, zipName string) *sync.WaitGroup {
	zipFile, err := os.Create(zipName)
	if err != nil {
		panic(err)
	}
	var zipWriterWg sync.WaitGroup
	zipWriterWg.Add(1)

	noOfZippedFiles := 0
	go func() {
		infoLogger.Print("Starting to zip files...")

		zipWriter := zip.NewWriter(zipFile)
		defer zipWriter.Close()

		defer zipFile.Close()
		for s3File := range s3FilesChannel {
			fileInfo, err := s3File.Stat()
			h := &zip.FileHeader{
				Name:fileInfo.Key,
				Method: zip.Deflate,
				Flags:  0x800,
			}
			f, _ := zipWriter.CreateHeader(h)

			_, err = io.Copy(f, s3File)
			if err != nil {
				errorLogger.Printf("Cannot add file to zip archive: %s", err)
				return
			}

			noOfZippedFiles++
		}
		infoLogger.Printf("Finished zipping up files. Number of zipped files is: %d", noOfZippedFiles)
	}()
	return &zipWriterWg
}

func zipFilesInParallel(s3Client *minio.Client, bucketName string, year int, s3ContentFolder string) {
	zipName := fmt.Sprintf("FT-archive-%d.zip", year)
	infoLogger.Print("Starting parallel zip creation process..")
	startTime := time.Now()

	doneCh := make(chan struct{})
	defer close(doneCh)

	s3Files := make(chan *minio.Object)
	zipWriterWg := writeZipFile(s3Files, zipName)

	var s3DownloadWg sync.WaitGroup
	s3ObjectKeyPrefix := fmt.Sprintf("%s/%d", s3ContentFolder, year)
	s3ListObjectsChannel := s3Client.ListObjects(bucketName, s3ObjectKeyPrefix, true, doneCh)
	for s3Object := range s3ListObjectsChannel {
		if s3Object.Err != nil {
			errorLogger.Printf("Error while receiving objectInfo: %s", s3Object.Err)
			continue
		}
		s3DownloadWg.Add(1)

		go func(s3FileName string) {
			defer s3DownloadWg.Done()
			obj, err := s3Client.GetObject(bucketName, s3FileName)
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
	infoLogger.Printf("Finished zip creation process. Duration: %s", zippingUpDuration)

	//upload zip file to s3
	infoLogger.Printf("Uploading file %s to s3...", zipName)
	zipFileToBeUploaded, err := os.Open(zipName)
	if err != nil {
		errorLogger.Printf("Could not open zip archive with name %s to upload it to S3. Error was: %s", zipName, err)
		return
	}
	defer zipFileToBeUploaded.Close()

	_, err = s3Client.PutObject(bucketName, fmt.Sprintf("yearly-archives/%s", zipName), zipFileToBeUploaded, "application/octet-stream")
	if err != nil {
		errorLogger.Printf("Could not upload file with name %s to s3. Error was: %s", zipName, err)
		return
	}

	infoLogger.Printf("Finished uploading file %s to s3", zipName)
}

func zipFilesInParallelLast30Days(s3Client *minio.Client, bucketName string, s3ContentFolder string) {
	zipName := "FT-archive-last-30-days.zip"
	infoLogger.Print("Starting parallel zip creation process..")
	startTime := time.Now()

	doneCh := make(chan struct{})
	defer close(doneCh)

	s3Files := make(chan *minio.Object)
	zipWriterWg := writeZipFile(s3Files, zipName)

	var s3DownloadWg sync.WaitGroup

	s3ListObjectsChannel := s3Client.ListObjects(bucketName, s3ContentFolder, true, doneCh)

	for s3Object := range s3ListObjectsChannel {
		if s3Object.Err != nil {
			errorLogger.Printf("Error while receiving objectInfo: %s", s3Object.Err)
			continue
		}

		//check if the date is less that thirty days ago.
		match := dateRegexp.FindStringSubmatch(s3Object.Key)
		if len(match) < 1 {
			errorLogger.Printf("Cannot parse date from s3 file name: %s", s3Object.Key)
			continue
		}

		s3FileDate, err := time.Parse(dateTemplate, match[0])
		if err != nil {
			errorLogger.Printf("Cannot parse date from s3 file name, error was: %s", err)
		}

		if !isLessThanThirtyDaysBefore(s3FileDate) {
			continue
		}

		s3DownloadWg.Add(1)

		go func(s3FileName string) {
			defer s3DownloadWg.Done()
			obj, err := s3Client.GetObject(bucketName, s3FileName)
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
	infoLogger.Printf("Finished zip creation process. Duration: %s", zippingUpDuration)
}

func isLessThanThirtyDaysBefore(date time.Time) bool {
	thirtyDays := time.Duration(30 * 24 * time.Hour)
	return time.Since(date) < thirtyDays
}
