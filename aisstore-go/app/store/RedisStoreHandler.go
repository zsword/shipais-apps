package store

import (
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/dhcc/aisstore-go/app/db"
	"github.com/dhcc/aisstore-go/config"
	"github.com/go-redis/redis"
)

type redisStoreHandler struct {
	ready   chan bool
	groupId string
}

func (consumer *redisStoreHandler) Setup(session sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *redisStoreHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *redisStoreHandler) SetReady() {
	consumer.ready = make(chan bool)
}

func (consumer *redisStoreHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	groupId := consumer.groupId
	const tkeyPrefix = config.AISTIME_PREFFIX
	const geoKey = config.AIS_GEO_KEY
	const keyExpire = config.AIS_KEY_EXPIRE * time.Second
	for message := range claim.Messages() {
		log.Printf("Message claimed: topc = %s, part = %d, %s, time = %v, size = %d", message.Topic, message.Partition, groupId, message.Timestamp, len(message.Value))
		list := []map[string]string{}
		msgValue := message.Value
		//msgValue = cleanJsonTextData(msgValue)
		err := json.Unmarshal(msgValue, &list)
		if err != nil {
			log.Printf("Error parse ais message: %s", err.Error())
			continue
		}
		startMS := time.Now().UnixMilli()
		timekeys := []string{}
		for _, v := range list {
			mmsi := v["mmsi"]
			timekeys = append(timekeys, tkeyPrefix+mmsi)
		}
		times, gerr := db.Redisdb.MGet(timekeys)
		if gerr != nil {
			log.Printf("[ERROR] Error get mmsi times: %s", gerr)
			continue
		}
		timePairs := []string{}
		dataPairs := []string{}
		geoList := []*redis.GeoLocation{}
		for i, v := range list {
			tval := times[i]
			tstr := v["receivetime"]
			rectime, perr := strconv.ParseUint(tstr, 10, 64)
			if perr != nil {
				log.Printf("[ERROR] Parse 'receivetime' error: %s", perr)
			}
			if tval != nil {
				time, terr := strconv.ParseUint(tval.(string), 10, 64)
				if terr != nil {
					log.Printf("[ERROR] Parse 'time value' error: %s", terr)
				}
				if time > rectime {
					continue
				}
			}
			mmsi := v["mmsi"]
			lon, lonerr := strconv.ParseFloat(v["longitude"], 64)
			if lonerr != nil {
				log.Printf("[ERROR] Parse 'longitude' error: %s", lonerr)
				continue
			}
			lat, laterr := strconv.ParseFloat(v["latitude"], 64)
			if laterr != nil {
				log.Printf("[ERROR] Parse 'latitude' error: %s", laterr)
				continue
			}
			timePairs = append(timePairs, tkeyPrefix+mmsi, tstr)
			data, jerr := json.Marshal(v)
			if jerr != nil {
				log.Printf("[ERROR] Marshal ais data error: %s", jerr)
			}
			dataPairs = append(dataPairs, mmsi, string(data))
			geoloc := redis.GeoLocation{
				Name:      mmsi,
				Longitude: lon,
				Latitude:  lat,
			}
			geoList = append(geoList, &geoloc)
		}
		endMS := time.Now().UnixMilli()
		if len(geoList) < 1 {
			log.Printf("No new data to redis: %d, times: %d", len(list), (endMS - startMS))
		}
		storePipe := db.Redisdb.TxPipeline()
		keys, serr := storePipe.MSet(timePairs).Result()
		if serr != nil {
			log.Printf("[ERROR] Save times to redis: %d, error: %s", len(list), serr)
		}
		keys, serr = storePipe.MSet(dataPairs).Result()
		if serr != nil {
			log.Printf("[ERROR] Save datalist to redis: %d, error: %s", len(list), serr)
		}
		geonum, geoerr := storePipe.GeoAdd(geoKey, geoList...).Result()
		if geoerr != nil {
			log.Printf("[ERROR] Save geolist to redis: %d, error: %s", len(list), geoerr)
		}
		for _, k := range timePairs {
			storePipe.Expire(k, keyExpire)
		}
		_, perr := storePipe.Exec()
		if perr != nil {
			log.Printf("[ERROR] Error redis data pipeliner: %d error: %s", len(list), perr)
			continue
		}
		endMS = time.Now().UnixMilli()
		log.Printf("Save to redis: %d, saved: %d, %d; %d, %d times: %d", len(list), len(timePairs), len(geoKey), len(keys), geonum, (endMS - startMS))
		session.MarkMessage(message, "")
	}
	return nil
}
