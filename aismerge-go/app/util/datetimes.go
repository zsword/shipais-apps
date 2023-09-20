package util

import "time"

var defaultLoc, _ = time.LoadLocation("Asia/Shanghai")

func ParseTime(layout string, str string) (value time.Time, err error) {
	value, err = time.ParseInLocation(layout, str, defaultLoc)
	return
}
