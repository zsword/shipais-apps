package store

import (
	"fmt"
	"time"

	"github.com/dhcc/aismerge-go/app/db"
	"github.com/dhcc/aismerge-go/app/logs"
	"github.com/dhcc/aismerge-go/app/model"
)

const aisMapping = `{
	"settings":{
		"number_of_shards": 3,
		"number_of_replicas": 0
	},
	"mappings": {
		"properties":{
			"mmsi": {"type": "keyword"},
			"receivetime": {"type": "long"},
			"geom": {"type": "geo_point"},
			"longitude": {"type": "double"},
			"latitude": {"type": "double"},
			"rot": {"type": "integer"},
			"sog": {"type": "integer"},
			"cog": {"type": "integer"},
			"thead": {"type": "integer"},
			"shipAndCargType": {"type": "keyword"},
			"draft": {"type": "float"}
		}
	}
}`
const (
	aisIndex   = "smcdm_ais"
	dateLayout = "2006-01"
	aisLayout  = "200601"
)

func InitElasticsDataIndex() (err error) {
	client := db.Elasticsdb
	curDate, err := time.Parse(dateLayout, "2021-01")
	if err != nil {
		return
	}
	endDate, err := time.Parse(dateLayout, "2023-12")
	if err != nil {
		return
	}
	for curDate.UnixMilli() <= endDate.UnixMilli() {
		aisName := aisIndex + curDate.Format(aisLayout)
		err = client.CreateIndex(aisName, aisMapping)
		if err != nil {
			return
		}
		curDate = curDate.AddDate(0, 1, 0)
	}
	return
}

type elasticsStore struct {
}

var ElasticsAisStore = elasticsStore{}

func (s elasticsStore) CountAis(aisName string, params map[string]interface{}) (count int64, err error) {
	esdb := db.Elasticsdb
	count, err = esdb.Count(aisName, params)
	return
}

func (s elasticsStore) CountAisByTime(beginTime time.Time, endTime time.Time) (count int64, err error) {
	countQ := make(map[string]interface{})
	timeFlt := map[string]interface{}{
		"gte": beginTime.UnixMilli(),
		"lt":  endTime.UnixMilli(),
	}
	timeFlt = map[string]interface{}{
		"receivetime": timeFlt,
	}
	countQ["query"] = map[string]interface{}{
		"range": timeFlt,
	}
	dbKeys := make(map[string]string)
	curTime := beginTime
	for curTime.UnixMilli() < endTime.UnixMilli() {
		timeKey := curTime.Format(aisLayout)
		aisName := aisIndex + timeKey
		dbKeys[aisName] = timeKey
		curTime = curTime.AddDate(0, 0, 1)
	}
	for k := range dbKeys {
		c, cerr := s.CountAis(k, countQ)
		if cerr != nil {
			err = cerr
			return
		}
		count += c
	}
	return
}

func (store elasticsStore) StoreAisElastics(records []model.ShipAisInfo) (count uint32, err error) {
	total := len(records)
	if total < 1 {
		return
	}
	logs.Info("Store AIS elastics: %d", total)
	startMS := time.Now().UnixMilli()
	dbKey := ""
	list := []map[string]interface{}{}
	for n, r := range records {
		v := r.ToMapData()
		mmsi := v["mmsi"]
		rectime := v["receivetime"].(int64)
		v["_id"] = fmt.Sprintf("%s,%d", mmsi, rectime)
		lat := v["latitude"].(float64)
		lon := v["longitude"].(float64)
		geom := map[string]float64{
			"lat": lat,
			"lon": lon,
		}
		v["geom"] = geom
		list = append(list, v)

		vtime := time.UnixMilli(rectime)
		aisKey := aisIndex + vtime.Format(aisLayout)
		if dbKey == "" {
			dbKey = aisKey
		} else if aisKey != dbKey {
			err = fmt.Errorf("multiple index keys")
			return
		}
		if len(list) < 100000 && n < (total-1) {
			continue
		}
		saves, serr := db.Elasticsdb.SaveAll(dbKey, list)
		list = list[:0]
		if serr == nil {
			count += saves
			logs.Info("Batch save ais data: %s, %d", dbKey, saves)
		} else {
			err = serr
			fmt.Println(err)
			return
		}
	}
	endMS := time.Now().UnixMilli()
	logs.Info("Store AIS elastics: %s, saved: %d, %d; times: %d", dbKey, total, count, (endMS - startMS))
	return
}
