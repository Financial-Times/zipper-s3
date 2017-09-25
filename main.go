package main

import (
	"fmt"
	"github.com/jawher/mow.cli"
	"github.com/minio/minio-go"
	"os"
	"sync"
	"time"
)

func main() {
	app := cli.App("Custom Zipper", "Zips files from S3")

	yearToStart := app.Int(cli.IntOpt{
		Name:   "year-to-start",
		Value:  1995,
		Desc:   "The app will create yearly zips starting from provided year. Defaults to 1995, when the first FT article has been published.",
		EnvVar: "YEAR_TO_START",
	})

	awsAccessKey := app.String(cli.StringOpt{
		Name:   "aws-access-key-id",
		Desc:   "S3 access key",
		EnvVar: "AWS_ACCESS_KEY_ID",
	})
	awsSecretKey := app.String(cli.StringOpt{
		Name:   "aws-secret-access-key",
		Desc:   "S3 secret key",
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
		Desc:   "S3 domain of content",
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
		infoLogger.Printf("Starting app with parameters: [s3-content-folder=%s], [bucket-name=%s] [year-to-start=%d]", *s3ContentFolder, *bucketName, *yearToStart)
		s3Client, err := minio.New(*s3Domain, *awsAccessKey, *awsSecretKey, true)
		if err != nil {
			errorLogger.Printf("error while creating s3client: %s", err.Error())
			os.Exit(1)
		}

		var zipperWg sync.WaitGroup
		errsCh := make(chan error)
		//zip files on a per year basis
		currentYear := time.Now().Year()
		startTime := time.Now()
		for year := currentYear; year >= *yearToStart; year-- {
			zipperWg.Add(1)
			go zipAndUploadFilesSequentially(s3Client, *bucketName, fmt.Sprintf("%s/%d", *s3ContentFolder, year), fmt.Sprintf("FT-archive-%d.zip", year), nil, &zipperWg, errsCh)
		}

		//zip files for last 30 days
		zipperWg.Add(1)
		go zipAndUploadFilesSequentially(s3Client, *bucketName, *s3ContentFolder, "FT-archive-last-30-days.zip", isContentLessThanThirtyDaysBefore, &zipperWg, errsCh)

		//todo: remove this:
		go func() {
			for {
				infoLogger.Print("heartbeat")
				time.Sleep(30 * time.Second)
			}
		}()

		go func() {
			err = <-errsCh
			if err != nil {
				errorLogger.Printf("Zip creation process finished with error: %s", err)
				os.Exit(1)
			}
		}()

		zipperWg.Wait()

		zippingUpDuration := time.Since(startTime)
		infoLogger.Printf("Finished creating all the archives. Total duration is: %s", zippingUpDuration)
	}

	err := app.Run(os.Args)
	if err != nil {
		errorLogger.Printf("Error while running app [%v]", err)
	}
}
