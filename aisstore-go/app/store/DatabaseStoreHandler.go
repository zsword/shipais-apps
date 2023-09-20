package store

import (
	"encoding/json"
	"log"
	"time"

	"github.com/Shopify/sarama"
	"github.com/dhcc/aisstore-go/app/db"
	"github.com/dhcc/aisstore-go/app/model"
	"github.com/dhcc/aisstore-go/app/util"
)

var snowflake *util.Snowflake

func InitDataIdGen() (err error) {
	snowflake, err = util.NewSnowflake(0)
	if err != nil {
		log.Panicf("[ERROR] Create Snowflake error: %s", err)
		return
	}
	return
}

type databaseStoreHandler struct {
	ready   chan bool
	groupId string
}

func (consumer *databaseStoreHandler) Setup(session sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *databaseStoreHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *databaseStoreHandler) SetReady() {
	consumer.ready = make(chan bool)
}

func (consumer *databaseStoreHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	groupId := consumer.groupId
	for message := range claim.Messages() {
		log.Printf("Message claimed: topc = %s, part = %d, %s, time = %v, size = %d", message.Topic, message.Partition, groupId, message.Timestamp, len(message.Value))
		startMS := time.Now().UnixMilli()
		list := []model.ShipAisInfo{}
		msgValue := message.Value
		//msgValue = cleanJsonTextData(msgValue)
		err := json.Unmarshal(msgValue, &list)
		if err != nil {
			log.Printf("Error parse ais message: %s", err.Error())
			continue
		}
		vmap := map[string][]model.ShipAisInfo{}
		for _, v := range list {
			v.BuildInfo()
			v.Id = snowflake.Generate()
			if v.Id == 0 {
				log.Panicf("[ERROR] ID is invliad: %d", v.Id)
			}
			dbKey := v.ShardingKey()
			vlist := vmap[dbKey]
			if vlist == nil {
				vlist = []model.ShipAisInfo{}
			}
			vlist = append(vlist, v)
			vmap[dbKey] = vlist
		}
		for k := range vmap {
			vlist := vmap[k]
			err := db.ShardingOrmClient.SaveInBatches(k, vlist)
			if err != nil {
				log.Printf("Error save ais batches: %v", err.Error())
				scount := 0
				fcount := 0
				err = nil
				for _, v := range vlist {
					//v.Id = 0
					serr := db.ShardingOrmClient.Save(k, v)
					if serr != nil {
						err = serr
						fcount++
						continue
					}
					scount++
				}
				endMS := time.Now().UnixMilli()
				log.Printf("Resave ais: %d, %d, %v, times: %d", scount, fcount, err, (endMS - startMS))
			} else {
				endMS := time.Now().UnixMilli()
				log.Printf("Save ais batches: %s, %d, times: %d", k, len(vlist), (endMS - startMS))
			}
		}
		session.MarkMessage(message, "")
	}
	return nil
}
