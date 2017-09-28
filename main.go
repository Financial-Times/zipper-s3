package main

import (
	"fmt"
	"github.com/jawher/mow.cli"
	"github.com/minio/minio-go"
	"os"
	"time"
)

func main() {
	app := cli.App("Custom Zipper", "Zips files from S3")
	maxNoOfGoroutines := app.Int(cli.IntOpt{
		Name:   "max-no-of-goroutines",
		Value:  3,
		Desc:   "The maximum number of goroutines which is used to zip files.",
		EnvVar: "MAX_NO_OF_GOROUTINES",
	})
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
		infoLogger.Printf("Starting app with parameters: [s3-content-folder=%s], [bucket-name=%s] [year-to-start=%d] [max-no-of-goroutines=%d]", *s3ContentFolder, *bucketName, *yearToStart, *maxNoOfGoroutines)
		s3Client, err := minio.New(*s3Domain, *awsAccessKey, *awsSecretKey, true)
		if err != nil {
			errorLogger.Printf("error while creating s3client: %s", err.Error())
			os.Exit(1)
		}

		s3Config := newS3Config(s3Client, *bucketName)

		errsCh := make(chan error)
		//zip files on a per year basis
		currentYear := time.Now().Year()
		startTime := time.Now()

		concurrentGoroutines := make(chan struct{}, *maxNoOfGoroutines)
		// Fill the dummy channel with maxNbConcurrentGoroutines empty struct.
		for i := 0; i < *maxNoOfGoroutines; i++ {
			concurrentGoroutines <- struct{}{}
		}

		// The done channel indicates when a single goroutine has
		// finished its job.
		done := make(chan bool)
		// The waitForAllJobs channel allows the main program
		// to wait until we have indeed done all the jobs.
		waitForAllJobs := make(chan bool)

		go func() {
			for year := *yearToStart; year <= currentYear; year++ {
				<-done
				// Say that another goroutine can now start.
				concurrentGoroutines <- struct{}{}
			}
			// We have collected all the jobs, the program
			// can now terminate
			waitForAllJobs <- true
		}()

		go func() {
			for {
				infoLogger.Printf("heartbeat [elapsed time: %s]", time.Since(startTime))
				time.Sleep(30 * time.Second)
			}
		}()

		for year := *yearToStart; year <= currentYear; year++ {
			infoLogger.Printf("Zipping up files from year %d waiting to launch!", year)
			<-concurrentGoroutines
			go zipAndUploadFiles(s3Config, fmt.Sprintf("%s/%d", *s3ContentFolder, year), fmt.Sprintf("FT-archive-%d.zip", year), nil, done, errsCh)
		}

		//wait for last archive to be finished.
		<-done

		//zip files for last 30 days
		go zipAndUploadFiles(s3Config, *s3ContentFolder, "FT-archive-last-30-days.zip", isContentLessThanThirtyDaysBefore, done, errsCh)

		go func() {
			err = <-errsCh
			if err != nil {
				errorLogger.Printf("Zip creation process finished with error: %s", err)
				os.Exit(1)
			}
		}()

		// Wait for all jobs to finish
		<-waitForAllJobs

		zippingUpDuration := time.Since(startTime)
		infoLogger.Printf("Finished creating all the archives. Total duration is: %s", zippingUpDuration)
	}

	err := app.Run(os.Args)
	if err != nil {
		errorLogger.Printf("Error while running app [%v]", err)
		os.Exit(1)
	}
}
