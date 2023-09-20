package main

import (
	"github.com/dhcc/aismerge-go/app/db"
	"github.com/dhcc/aismerge-go/app/store"
	"github.com/dhcc/aismerge-go/config"
)

func main() {
	err := config.InitConfig()
	if err != nil {
		return
	}
	err = db.InitOrmDB()
	if err != nil {
		return
	}
	err = db.InitElastics()
	if err != nil {
		return
	}
	err = store.InitElasticsDataIndex()
	if err != nil {
		return
	}
	count := 12
	err = store.InitAisMergeTask(count)
	if err != nil {
		return
	}
}
