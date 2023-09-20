package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type redisProps struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database int    `json:"database"`
}

type appConfig struct {
	Mode       string `json:"mode"`
	Port       string `json:"port"`
	redisProps `json:"redis"`
}

var (
	AppConfig  *appConfig
	RedisProps *redisProps
)

func InitConfig() (err error) {
	file, _ := os.Open("config.json")
	defer file.Close()

	decoder := json.NewDecoder(file)

	err = decoder.Decode(&AppConfig)
	if err != nil {
		fmt.Println(err)
		return
	}

	RedisProps = &AppConfig.redisProps
	if RedisProps.Host == "" {
		RedisProps.Host = "127.0.0.1"
	}
	if RedisProps.Port <= 0 {
		RedisProps.Port = 6379
	}
	return
}
