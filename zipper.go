package main

import (
	"github.com/minio/minio-go"
	"time"
	"archive/zip"
	"os"
	"io"
)

func addFileToZip(s3Client *minio.Client, bucketName string, fileName string, zipWriter *zip.Writer) {
	obj, err := s3Client.GetObject(bucketName, fileName)
	if err != nil {
		errorLogger.Printf("Cannot download file with name %s from s3: %s", fileName, err)
		return
	}

	defer obj.Close()
	infoLogger.Printf("downloaded file with name: %s", fileName)

	h := &zip.FileHeader{
		Name:   fileName,
		Method: zip.Deflate,
		Flags:  0x800,
	}
	f, _ := zipWriter.CreateHeader(h)

	_, err = io.Copy(f, obj)
	if err != nil {
		errorLogger.Printf("Cannot add file to zip archive: %s", err)
		return
	}

	infoLogger.Printf("Added file with name [%s] to zip archive.", fileName)
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

	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	objectCh := s3Client.ListObjects(bucketName, "", true, doneCh)
	for object := range objectCh {
		if object.Err != nil {
			errorLogger.Printf("Error while receiving objectInfo: %s", object.Err)
			continue
		}
		go addFileToZip(s3Client, bucketName, object.Key, zipWriter)
		noOfZippedFiles++
	}

	zippingUpDuration := time.Since(startTime)
	infoLogger.Printf("Finished zip creation process. Duration: %s. No of zipped files: %d", zippingUpDuration, noOfZippedFiles)
}
