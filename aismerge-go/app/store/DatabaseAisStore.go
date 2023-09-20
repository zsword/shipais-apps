package store

import (
	"log"
	"time"

	"github.com/dhcc/aismerge-go/app/db"
	"github.com/dhcc/aismerge-go/app/model"
	"github.com/dhcc/aismerge-go/app/util"
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

func StoreAisDB(list []model.ShipAisInfo) (count int, err error) {
	log.Printf("Store AIS DB: %d", len(list))
	startMS := time.Now().UnixMilli()
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
				serr := db.ShardingOrmClient.SaveSharding(k, v)
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
	return
}
