package store

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

type storeTopics struct {
	AisCableTopic  string
	AisMarineTopic string
	AisLandTopic   string
}

var StoreTopics = storeTopics{
	AisCableTopic:  "ais-cable",
	AisMarineTopic: "ais-marine",
	AisLandTopic:   "ais-land",
}

type storeEngine struct {
	Database string
	Redis    string
	Elastics string
}

var StoreEngine = storeEngine{
	Database: "Database",
	Redis:    "Redis",
	Elastics: "Elastics",
}

type storeGroups struct {
	Redis         string
	PostgreSQL    string
	ElasticSearch string
}

var StoreGroups = storeGroups{
	Redis:         "redis",
	PostgreSQL:    "postgresql",
	ElasticSearch: "elasticsearch",
}

func cleanJsonTextData(data []byte) (res []byte) {
	str := string(data)
	str = strings.ReplaceAll(str, "/\"", "\\\"")
	return []byte(str)
}

func jsonConv() {
	file, ferr := os.Open("D:/DemoData/Json-Error.txt")
	if ferr != nil {
		fmt.Println(ferr)
		return
	}
	defer file.Close()
	bytes, rerr := ioutil.ReadAll(file)
	str := string(bytes)
	str = strings.ReplaceAll(str, "/\"", "\\\"")
	if rerr != nil {
		fmt.Println(rerr)
	}
	list := []map[string]string{}
	jerr := json.Unmarshal([]byte(str), &list)
	if jerr != nil {
		fmt.Println(jerr)
		return
	}
	return
}
