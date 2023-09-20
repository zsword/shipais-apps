package main

import (
	"fmt"

	"github.com/dhcc/aisstore-go/app/db"
	"github.com/dhcc/aisstore-go/app/kafka"
	"github.com/dhcc/aisstore-go/app/store"
	"github.com/dhcc/aisstore-go/config"
)

func main() {
	err := config.InitConfig()
	if err != nil {
		return
	}
	err = kafka.InitKafka()
	if err != nil {
		return
	}
	storeEngine := config.AppConfig.StoreEngine
	StoreTypes := store.StoreEngine
	StoreGroups := store.StoreGroups
	groupId := StoreGroups.Redis
	switch storeEngine {
	case StoreTypes.Database:
		err = db.InitOrmDB()
		// err := db.InitPostgreDB()
		// if err != nil {
		// 	return
		// }
		if err != nil {
			return
		}
		err = store.InitDataIdGen()
		if err != nil {
			return
		}
		groupId = StoreGroups.PostgreSQL
	case StoreTypes.Elastics:
		err = db.InitElastics()
		if err != nil {
			return
		}
		err = store.InitElasticsDataIndex()
		if err != nil {
			return
		}
		groupId = StoreGroups.ElasticSearch
	default:
		err = db.InitRedis()
		if err != nil {
			return
		}
	}
	err = store.InitAisStoreHandler(groupId)
	if err != nil {
		return
	}
	fmt.Println("Exit")
}
