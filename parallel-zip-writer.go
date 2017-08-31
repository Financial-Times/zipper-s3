package main

import (
	"time"
	"os"
	"sync"
	"github.com/minio/minio-go"
	gzip "github.com/klauspost/pgzip"
	"archive/tar"
	"io"
)

func writeZipFile(s3FilesChannel chan *minio.Object) *sync.WaitGroup {
	zipFile, err := os.Create("out.zip")
	if err != nil {
		panic(err)
	}
	var zipWriterWg sync.WaitGroup
	zipWriterWg.Add(1)

	noOfZippedFiles := 0
	go func() {
		infoLogger.Print("Starting to zip files...")

		//compress the tar archive
		gzipWriter, err := gzip.NewWriterLevel(zipFile, gzip.BestSpeed)
		if err != nil {
			errorLogger.Printf("Failed to create gzip writer : %s", err)
			return
		}
		defer gzipWriter.Close()
		//create a tar archive
		tarWriter := tar.NewWriter(gzipWriter)
		defer tarWriter.Close()

		// Note the order (LIFO):
		defer zipWriterWg.Done() // 2. signal that we're done
		defer zipFile.Close() // 1. close the file
		for s3File := range s3FilesChannel {
			fileInfo, _ := s3File.Stat()

			fileInfoHeader := &tar.Header{
				Name:    fileInfo.Key,
				Size: fileInfo.Size,
				//ModTime: fi.ModTime(),
				//Mode:    int64(fm.Perm()),
			}

			err = tarWriter.WriteHeader(fileInfoHeader)
			if err != nil {
				errorLogger.Printf("Cannot write tar header, error: %s", err)
				continue
			}

			_, err = io.Copy(tarWriter, s3File)
			if err != nil {
				errorLogger.Printf("Cannot add file to archive, error: %s", err)
				continue
			}

			infoLogger.Printf("Added file with name %s to archive", fileInfo.Key)
			noOfZippedFiles++
		}
		infoLogger.Printf("Finished zipping up files. Number of zipped files is: %d", noOfZippedFiles)
	}()
	return &zipWriterWg
}

func zipFilesInParallel(s3Client *minio.Client, bucketName string, zipFileName string) {
	infoLogger.Print("Starting parallel zip creation process..")
	startTime := time.Now()

	doneCh := make(chan struct{})
	defer close(doneCh)

	s3Files := make(chan *minio.Object)
	zipWriterWg := writeZipFile(s3Files)

	// Send all files to the zip writer.
	var s3DownloadWg sync.WaitGroup

	s3ListObjectsChannel := s3Client.ListObjects(bucketName, "", true, doneCh)
	for s3Object := range s3ListObjectsChannel {
		if s3Object.Err != nil {
			errorLogger.Printf("Error while receiving objectInfo: %s", s3Object.Err)
			continue
		}
		s3DownloadWg.Add(1)
		// Read each file in parallel:
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
	// Once we're done sending the files, we can close the channel.
	close(s3Files)
	// This will cause ZipWriter to break out of the loop, close the file,
	// and unblock the next mutex:
	zipWriterWg.Wait()
	zippingUpDuration := time.Since(startTime)
	infoLogger.Printf("Finished zip creation process. Duration: %s", zippingUpDuration)
}
