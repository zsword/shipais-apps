package model

import (
	"time"

	"github.com/dhcc/aismerge-go/app/types"
)

type MergeInfo struct {
	TimeBegin  int64 `gorm:"primaryKey"`
	TimeEnd    int64 `gorm:"primaryKey"`
	Status     int
	StartTime  types.Time
	FinishTime types.Time
	Total      int64
	Count      int64
}

func (m *MergeInfo) SetStartTime(t time.Time) {
	m.StartTime = types.Time(t)
}

func (m *MergeInfo) SetFinishTime(t time.Time) {
	m.FinishTime = types.Time(t)
}
