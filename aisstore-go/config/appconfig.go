package config

import (
	"encoding/json"
	"log"
	"os"
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

type aisProps struct {
	Region string `json:"region"`
}

type appConfig struct {
	Kafka       *kafkaProps    `json:"kafka"`
	Database    *dbProps       `json:"db"`
	Redis       *redisProps    `json:"redis"`
	Elastics    *elasticsProps `json:"elastics"`
	AisInfo     *aisProps      `json:"ais"`
	StoreEngine string         `json:storeEngine`
}

var AppConfig *appConfig

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
	return
}
