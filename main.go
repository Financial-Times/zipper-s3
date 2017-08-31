package main

import (
	"github.com/jawher/mow.cli"
	"github.com/minio/minio-go"
	"os"
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
		Desc:   "bucket name of factset data",
		EnvVar: "BUCKET_NAME",
	})
	s3Domain := app.String(cli.StringOpt{
		Name:   "s3-domain",
		Value:  "s3.amazonaws.com",
		Desc:   "s3 domain of factset bucket",
		EnvVar: "S3_DOMAIN",
	})

	app.Action = func() {
		initLogs(os.Stdout, os.Stdout, os.Stderr)
		s3Client, err := minio.New(*s3Domain, *awsAccessKey, *awsSecretKey, true)
		if err != nil {
			errorLogger.Printf("error while creating s3client: %s", err.Error())
			os.Exit(1)
		}

		zipFileName := "target.zip"

		zipFilesInParallel(s3Client, *bucketName, zipFileName)
	}

	err := app.Run(os.Args)
	if err != nil {
		errorLogger.Printf("Error while running app [%v]", err)
	}
}