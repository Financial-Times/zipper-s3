package main

import (
	"archive/tar"
	"compress/gzip"
	"github.com/minio/minio-go"
	"io"
	"os"
	"time"
)

func addFileToZip(s3Client *minio.Client, bucketName string, fileName string, tarWriter *tar.Writer) {
	s3File, err := s3Client.GetObject(bucketName, fileName)
	if err != nil {
		errorLogger.Printf("Cannot download file with name %s from s3: %s", fileName, err)
		return
	}

	defer s3File.Close()
	infoLogger.Printf("downloaded file with name: %s", fileName)

	fileInfo, _ := s3File.Stat()

	fileInfoHeader := &tar.Header{
		Name: fileInfo.Key,
		Size: fileInfo.Size,
		//todo: add mode.
		//Mode:    int64(fm.Perm()),
	}

	err = tarWriter.WriteHeader(fileInfoHeader)
	if err != nil {
		errorLogger.Printf("Cannot write tar header, error: %s", err)
		return
	}

	_, err = io.Copy(tarWriter, s3File)
	if err != nil {
		errorLogger.Printf("Cannot add file to archive, error: %s", err)
		return
	}

	infoLogger.Printf("Added file with name %s to archive", fileInfo.Key)
}

func zipFiles(s3Client *minio.Client, bucketName string, zipFileName string) {
	infoLogger.Print("Starting zip creation process..")
	startTime := time.Now()
	noOfZippedFiles := 0
	doneCh := make(chan struct{})
	defer close(doneCh)

	zipFile, err := os.Create(zipFileName)
	if err != nil {
		errorLogger.Printf("Cannot create zip file with name [%s]. Error was: %s", zipFileName, err)
	}
	defer zipFile.Close()
	//defer os.Remove(zipFile.Path)

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

	objectCh := s3Client.ListObjects(bucketName, "", true, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			errorLogger.Printf("Error while receiving objectInfo: %s", object.Err)
			continue
		}
		addFileToZip(s3Client, bucketName, object.Key, tarWriter)
		noOfZippedFiles++
	}

	zippingUpDuration := time.Since(startTime)
	infoLogger.Printf("Finished zip creation process. Duration: %s. No of zipped files: %d", zippingUpDuration, noOfZippedFiles)
}
