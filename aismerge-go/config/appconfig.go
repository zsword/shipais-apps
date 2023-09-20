package config

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/dhcc/aismerge-go/app/util"
)

type kafkaProps struct {
	Addrs []string `json:"addrs"`
}

type dbProps struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Dbname   string `json:"dbname"`
}

type redisProps struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database int    `json:"database"`
}

type elasticsProps struct {
	Addresses []string `json:"addresses"`
	User      string   `json:"user"`
	Password  string   `json:"password"`
	CloudID   string   `json:"cloudId"`
	ApiKey    string   `json:"apiKey"`
}

type mergeProps struct {
	BeginTimeStr string `json:"beginTime"`
	EndTimeStr   string `json:"endTime"`
	TimeBegin    time.Time
	TimeEnd      time.Time
}

type appConfig struct {
	Kafka       *kafkaProps    `json:"kafka"`
	Database    *dbProps       `json:"db"`
	Redis       *redisProps    `json:"redis"`
	Elastics    *elasticsProps `json:"elastics"`
	MergeInfo   *mergeProps    `json:"merge"`
	StoreEngine string         `json:storeEngine`
}

var AppConfig *appConfig

const DATE_LAYOUT = "2006-01-02"

func InitConfig() (err error) {
	AppConfig = &appConfig{}

	file, ferr := os.Open("./appconfig.json")
	if ferr != nil {
		log.Panicf("[ERROR] Open config error: %s", ferr)
		err = ferr
		return
	}
	defer file.Close()
	json := json.NewDecoder(file)

	err = json.Decode(&AppConfig)
	if err != nil {
		log.Panicf("[ERROR] Load config error: %s", err)
	}
	mergeInfo := AppConfig.MergeInfo
	if mergeInfo.BeginTimeStr != "" {
		mergeInfo.TimeBegin, _ = util.ParseTime(DATE_LAYOUT, mergeInfo.BeginTimeStr)
	}
	if mergeInfo.EndTimeStr != "" {
		mergeInfo.TimeEnd, _ = util.ParseTime(DATE_LAYOUT, mergeInfo.EndTimeStr)
	}
	return
}
