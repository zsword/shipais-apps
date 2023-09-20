package logs

import (
	"log"
)

const (
	infoKey  = "[info] "
	warnKey  = "[warn] "
	errorKey = "[err] "
)

func Warn(format string, v ...interface{}) {
	log.Printf(warnKey+format, v...)
}

func Error(format string, v ...interface{}) {
	log.Printf(errorKey+format, v...)
}

func Info(format string, v ...interface{}) {
	log.Printf(infoKey+format, v...)
}
