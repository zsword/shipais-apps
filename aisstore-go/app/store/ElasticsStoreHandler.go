package store

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/dhcc/aisstore-go/app/db"
	"github.com/dhcc/aisstore-go/app/logs"
)

type elasticsStoreHandler struct {
	ready   chan bool
	groupId string
}

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

func (consumer *elasticsStoreHandler) Setup(session sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *elasticsStoreHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *elasticsStoreHandler) SetReady() {
	consumer.ready = make(chan bool)
}

func (consumer *elasticsStoreHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	groupId := consumer.groupId
	for message := range claim.Messages() {
		log.Printf("Message claimed: topc = %s, part = %d, %s, time = %v, size = %d", message.Topic, message.Partition, groupId, message.Timestamp, len(message.Value))
		list := []map[string]interface{}{}
		msgValue := message.Value
		//msgValue = cleanJsonTextData(msgValue)
		err := json.Unmarshal(msgValue, &list)
		if err != nil {
			log.Printf("Error parse ais message: %s", err.Error())
			continue
		}
		startMS := time.Now().UnixMilli()
		vmap := make(map[string][]interface{})
		for _, v := range list {
			mmsi := v["mmsi"]
			rectime := v["receivetime"].(string)
			v["_id"] = fmt.Sprintf("%s,%s", mmsi, rectime)
			latVal := v["latitude"].(string)
			lonVal := v["longitude"].(string)
			lat, err := strconv.ParseFloat(latVal, 64)
			if err != nil {
				logs.Warn("Parse ais latitude: %s, %s", v["lati"])
				continue
			}
			lon, err := strconv.ParseFloat(lonVal, 64)
			if err != nil {
				logs.Warn("Parse ais latitude: %s, %s", v["lati"])
				continue
			}
			geom := map[string]float64{
				"lat": lat,
				"lon": lon,
			}
			v["geom"] = geom
			timeVal, _ := strconv.ParseInt(rectime, 0, 64)
			vtime := time.UnixMilli(timeVal)
			dbKey := aisIndex + vtime.Format(aisLayout)
			vlist := vmap[dbKey]
			if vlist == nil {
				vlist = []interface{}{}
			}
			vlist = append(vlist, v)
			vmap[dbKey] = vlist
		}
		saves := uint32(0)
		for k := range vmap {
			vlist := vmap[k]
			saves, err = db.Elasticsdb.SaveAll(k, vlist)
			if err == nil {
				logs.Info("Batch save ais data: %d", saves)
			}
		}
		endMS := time.Now().UnixMilli()
		log.Printf("Save to elastics: %s, saved: %d, %d; times: %d", aisIndex, len(list), saves, (endMS - startMS))
		session.MarkMessage(message, "")
	}
	return nil
}
