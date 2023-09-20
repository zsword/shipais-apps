package store

import (
	"fmt"
	"sync"
	"time"

	"github.com/dhcc/aismerge-go/app/db"
	"github.com/dhcc/aismerge-go/app/logs"
	"github.com/dhcc/aismerge-go/app/model"
	"github.com/dhcc/aismerge-go/app/util"
	"github.com/dhcc/aismerge-go/config"
)

func dbToElastis(beginTime time.Time, hours int, wg *sync.WaitGroup) (err error) {
	defer wg.Done()
	ormc := db.ShardingOrmClient
	ess := ElasticsAisStore
	endTime := beginTime.Add(time.Duration(hours) * time.Hour)
	dbKey := model.BuildShardingKey(beginTime)
	params := map[string]interface{}{}
	params["jssjc BETWEEN ? AND ?"] = []time.Time{beginTime, endTime}
	total, err := ormc.Count(dbKey, params)
	if err != nil {
		return err
	}
	if total < 1 {
		return
	}
	logs.Info("Start AIS merge job: %v, %d, %d", beginTime, hours, total)
	minfo := model.MergeInfo{
		TimeBegin: beginTime.UnixMilli(),
		TimeEnd:   endTime.UnixMilli(),
	}
	err = ormc.FindOne(&minfo)
	if err != nil {
		return
	}
	if minfo.Status == 1 && minfo.Count == total {
		return
	}
	minfo.Total = total
	esCount, err := ess.CountAisByTime(beginTime, endTime)
	if esCount == total {
		minfo.Status = 1
		minfo.Count = esCount
		ormc.Save(&minfo)
		logs.Info("All data saved to ES: %d, %d", total, esCount)
		return
	} else {
		minfo.Status = 2
		minfo.Count = 0
		minfo.SetStartTime(time.Now())
	}
	ormc.Save(&minfo)
	minutes := hours * 60
	dataTime := beginTime
	step := 5
	stepDuration := time.Duration(step) * time.Minute
	saved := int64(0)
	for m := 0; m < minutes; m += step {
		fromTime := dataTime
		dataTime = dataTime.Add(stepDuration)
		dataParam := map[string]interface{}{}
		dataParam["jssjc BETWEEN ? AND ?"] = []time.Time{fromTime, dataTime}
		dataKey := model.BuildShardingKey(fromTime)
		var list []model.ShipAisInfo
		ferr := ormc.FindSharding(dataKey, dataParam, &list)
		if ferr != nil {
			err = ferr
			return
		}
		if len(list) < 1 {
			continue
		}
		sc, err := ess.StoreAisElastics(list)
		if err != nil {
			fmt.Println(err)
		}
		saved += int64(sc)
		minfo.SetFinishTime(time.Now())
		minfo.Count = saved
		if saved == total {
			minfo.Status = 1
		}
		ormc.Save(&minfo)
	}
	return
}

func InitAisMergeTask(count int) (err error) {
	timeLayout := "2006-01-02"
	ormc := db.ShardingOrmClient
	hours := 1
	wg := sync.WaitGroup{}
	beginInfo := model.MergeInfo{
		Status: 2,
	}
	cfg := config.AppConfig.MergeInfo
	beginMS := cfg.TimeBegin.UnixMilli()
	if beginMS < 1 {
		beginTime, _ := util.ParseTime(timeLayout, "2021-01-01")
		beginMS = beginTime.UnixMilli()
	}
	err = ormc.Find(&beginInfo, "time_begin ASC", 0, 1)
	if beginInfo.Total < 1 {
		beginInfo.Status = 1
		err = ormc.Find(&beginInfo, "time_begin DESC", 0, 1)
	}
	if beginInfo.TimeEnd > 0 {
		if beginInfo.Status == 1 {
			beginMS = beginInfo.TimeEnd
		} else {
			beginMS = beginInfo.TimeBegin
		}
	}
	dataTime := time.UnixMilli(beginMS)
	hourDuration := time.Duration(hours) * time.Hour
	endTime := cfg.TimeEnd
	if endTime.UnixMilli() < 1 {
		endTime = time.Now().Add(-hourDuration)
		endTime, err = util.ParseTime(timeLayout, "2023-01-01")
	}
	jobCount := 0
	logs.Info("AIS data merge task started: %v ~ %v", dataTime, endTime)
	startMS := time.Now().UnixMilli()
	month := time.Month(0)
	for dataTime.Before(endTime) {
		for n := 0; n < count; n++ {
			wg.Add(1)
			go dbToElastis(dataTime, hours, &wg)
			curMonth := dataTime.Month()
			if month != curMonth {
				month = curMonth
				fmt.Println("Task AIS Time: ", dataTime)
			}
			dataTime = dataTime.Add(hourDuration)
			jobCount++
			if !dataTime.Before(endTime) {
				break
			}
		}
		wg.Wait()
	}
	endMS := time.Now().UnixMilli()
	logs.Info("Execute AIS data merge jobs: %d, times: %f", jobCount, (float64)(endMS-startMS)/1000)
	logs.Info("Finish AIS data merge task")
	ret := ""
	fmt.Scan(&ret)
	return
}
