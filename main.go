package main

import (
	"github.com/jawher/mow.cli"
	"github.com/minio/minio-go"
	"os"
	"fmt"
	"time"
)

func main() {
	app := cli.App("Custom Zipper", "Zips files from S3")

	awsAccessKey := app.String(cli.StringOpt{
		Name:   "aws-access-key-id",
		Desc:   "s3 access key",
		EnvVar: "AWS_ACCESS_KEY_ID",
	})
	awsSecretKey := app.String(cli.StringOpt{
		Name:   "aws-secret-access-key",
		Desc:   "s3 secret key",
		EnvVar: "AWS_SECRET_ACCESS_KEY",
	})
	bucketName := app.String(cli.StringOpt{
		Name:   "bucket-name",
		Desc:   "bucket name of content",
		EnvVar: "BUCKET_NAME",
	})
	s3Domain := app.String(cli.StringOpt{
		Name:   "s3-domain",
		Value:  "s3.amazonaws.com",
		Desc:   "s3 domain of content",
		EnvVar: "S3_DOMAIN",
	})

	s3ContentFolder := app.String(cli.StringOpt{
		Name:   "s3-content-folder",
		Value:  "unarchived-content",
		Desc:   "Name of the folder that json files with the content are stored in.",
		EnvVar: "S3_CONTENT_FOLDER",
	})

	app.Action = func() {
		initLogs(os.Stdout, os.Stdout, os.Stderr)
		infoLogger.Printf("Starting app with parameters: [s3-content-folder=%s], [bucket-name=%s]", *s3ContentFolder, *bucketName)
		s3Client, err := minio.New(*s3Domain, *awsAccessKey, *awsSecretKey, true)
		if err != nil {
			errorLogger.Printf("error while creating s3client: %s", err.Error())
			os.Exit(1)
		}

		currentYear := time.Now().Year()
		zipFilesInParallel(s3Client, *bucketName, fmt.Sprintf("%s/%d", *s3ContentFolder, currentYear), fmt.Sprintf("FT-archive-%d.zip", currentYear), nil)
		zipFilesInParallel(s3Client, *bucketName, fmt.Sprintf("%s/%d", *s3ContentFolder, currentYear - 1), fmt.Sprintf("FT-archive-%d.zip", currentYear - 1), nil)
		err = zipFilesInParallel(s3Client, *bucketName, *s3ContentFolder, "FT-archive-last-30-days.zip", isContentLessThanThirtyDaysBefore)
		if err != nil {
			errorLogger.Printf("Zip creation process finished with error: %s", err)
		}
		os.Exit(1)
	}

	err := app.Run(os.Args)
	if err != nil {
		errorLogger.Printf("Error while running app [%v]", err)
	}
}
