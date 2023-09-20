package types

import (
	"fmt"
	"time"
)

type Time time.Time

func (t Time) String() string {
	return time.Time(t).String()
}

func (t *Time) Scan(v interface{}) error {
	value, ok := v.(time.Time)
	if ok {
		*t = Time(value.Add(time.Duration(-8) * time.Hour))
		//*t = Time(value.In(time.UTC))
		return nil
	}
	return fmt.Errorf("can not convert %v to timestamp", v)
}
