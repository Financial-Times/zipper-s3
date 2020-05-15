package main

import (
	"fmt"
	standardlog "log"
	"os"
	"time"

	"github.com/Shopify/sarama"
	cli "github.com/jawher/mow.cli"
	minio "github.com/minio/minio-go/v6"
	log "github.com/sirupsen/logrus"
)

const (
	last30DaysArchiveName    = "FT-archive-last-30-days.zip"
	yearlyArchivesNameFormat = "FT-archive-%d.zip"
	conceptsArchiveName      = "FT-archive-concepts.zip"
)

func main() {
	app := cli.App("Custom Zipper", "Zips files from S3")
	isAppEnabled := app.Bool(cli.BoolOpt{
		Name:   "is-enabled",
		Value:  false,
		Desc:   "Flag representing whether the app should run.",
		EnvVar: "IS_ENABLED",
	})
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

	s3ConceptFolder := app.String(cli.StringOpt{
		Name:   "s3-concept-folder",
		Value:  "unarchived-concepts",
		Desc:   "Name of the folder that json files with the concepts are stored in.",
		EnvVar: "S3_CONCEPT_FOLDER",
	})

	s3ContentFolder := app.String(cli.StringOpt{
		Name:   "s3-content-folder",
		Value:  "unarchived-content",
		Desc:   "Name of the folder that json files with the content are stored in.",
		EnvVar: "S3_CONTENT_FOLDER",
	})

	s3ArchivesFolder := app.String(cli.StringOpt{
		Name:   "s3-archives-folder",
		Value:  "test-yearly-archives",
		Desc:   "Name of the folder where the zip files will be placed.",
		EnvVar: "S3_ARCHIVES_FOLDER",
	})

	logDebug := app.Bool(cli.BoolOpt{
		Name:   "logDebug",
		Value:  false,
		Desc:   `Flag which if it is set to true, the app will also output debug logs.`,
		EnvVar: "LOG_DEBUG",
	})

	log.SetLevel(log.InfoLevel)

	app.Action = func() {
		if *logDebug {
			sarama.Logger = standardlog.New(os.Stdout, "[sarama] ", standardlog.LstdFlags)
			log.SetLevel(log.DebugLevel)
		}

		log.Infof("Starting app with parameters: [s3-content-folder=%s],[s3-concepts-folder=%s], [s3-archives-folder=%s], [bucket-name=%s] [year-to-start=%d] [max-no-of-goroutines=%d] [is-enabled: %t]", *s3ContentFolder, *s3ConceptFolder, *s3ArchivesFolder, *bucketName, *yearToStart, *maxNoOfGoroutines, *isAppEnabled)

		if !*isAppEnabled {
			log.Infof("App is not enabled. Please enable it by setting the IS_ENABLED env var.")
			return
		}

		s3Client, err := minio.New(*s3Domain, *awsAccessKey, *awsSecretKey, true)
		if err != nil {
			log.WithError(err).Fatal("Cannot create S3 client")
		}

		s3Config := newS3Config(s3Client, *bucketName, *s3ContentFolder, *s3ConceptFolder, *s3ArchivesFolder)

		startTime := time.Now()
		go func() {
			for {
				log.Infof("heartbeat [elapsed time: %s]", time.Since(startTime))
				time.Sleep(30 * time.Second)
			}
		}()

		//concepts zipping
		conceptFileKeys, err := s3Config.getFileKeys(s3Config.conceptFolderName)
		if err != nil {
			log.WithError(err).Fatal("Cannot get file keys from s3")
		}

		//contents zipping
		contentFileKeys, err := s3Config.getFileKeys(s3Config.contentFolderName)
		if err != nil {
			log.WithError(err).Fatal("Cannot get file keys from s3")
		}

		errsCh := make(chan error)
		//zip files on a per year basis
		currentYear := time.Now().Year()

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
			<-done
			// Say that another goroutine can now start.
			concurrentGoroutines <- struct{}{}

			for year := *yearToStart; year <= currentYear; year++ {
				<-done
				// Say that another goroutine can now start.
				concurrentGoroutines <- struct{}{}
			}
			// We have collected all the jobs, the program
			// can now terminate
			waitForAllJobs <- true
		}()

		log.Infof("Zipping up files for concepts waiting to launch!")
		<-concurrentGoroutines
		zipConfig := newZipConfig(conceptsArchiveName, nil, 0, conceptFileKeys)
		go zipAndUploadFiles(s3Config, zipConfig, done, errsCh)

		for year := *yearToStart; year <= currentYear; year++ {
			log.Infof("Zipping up files from year %d waiting to launch!", year)
			<-concurrentGoroutines

			zipConfig := newZipConfig(fmt.Sprintf(yearlyArchivesNameFormat, year), isContentFromProvidedYear, year, contentFileKeys)
			go zipAndUploadFiles(s3Config, zipConfig, done, errsCh)
		}

		//wait for last archive to be finished.
		<-done

		//zip files for last 30 days
		zipConfig = newZipConfig(last30DaysArchiveName, isContentLessThanThirtyDaysBefore, 0, contentFileKeys)
		go zipAndUploadFiles(s3Config, zipConfig, done, errsCh)

		go func() {
			err = <-errsCh
			if err != nil {
				log.WithError(err).Fatal("Zip creation process finished with error")
			}
		}()

		// Wait for all jobs to finish
		<-waitForAllJobs

		zippingUpDuration := time.Since(startTime)
		log.Infof("Finished creating all the archives. Total duration is: %s", zippingUpDuration)
	}

	err := app.Run(os.Args)
	if err != nil {
		log.WithError(err).Fatal("Error while running app")
	}
}

func init() {
	f := &log.JSONFormatter{
		TimestampFormat: time.RFC3339Nano,
	}

	log.SetFormatter(f)
}
