package main

import (
	"io"
	"log"
)

var infoLogger *log.Logger
var warnLogger *log.Logger
var errorLogger *log.Logger

const logPattern = log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile | log.LUTC

func initLogs(infoHandle io.Writer, warnHandle io.Writer, errorHandle io.Writer) {
	//to be used for INFO-level logging: InfoLogger.Println("foo is now bar")
	infoLogger = log.New(infoHandle, "INFO  - ", logPattern)
	//to be used for WARN-level logging: warnLogger.Println("foo is now bar")
	warnLogger = log.New(warnHandle, "WARN  - ", logPattern)
	//to be used for ERROR-level logging: errorLogger.Println("foo is now bar")
	errorLogger = log.New(errorHandle, "ERROR - ", logPattern)
}
