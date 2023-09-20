package db

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/dhcc/aismerge-go/config"
	_ "github.com/lib/pq"
)

type shardingDbClient struct {
}

var ShardingDb shardingDbClient

var dbMap map[string]*sql.DB

func InitPostgreDB() (err error) {
	ShardingDb = shardingDbClient{}
	dbMap = map[string]*sql.DB{}
	props := config.AppConfig.Database

	for y := 2021; y < 2024; y++ {
		dbkey := fmt.Sprintf("%s%d", props.Dbname, y)
		params := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", props.Host, props.Port, props.User, props.Password, dbkey)
		sdb, serr := sql.Open("postgres", params)
		if serr != nil {
			log.Panicf("Error connect db: %s, %v", params, err)
			err = serr
			return
		}
		defer sdb.Close()
		dbMap[dbkey] = sdb
	}
	rows, err := ShardingDb.Query("select * from cbztxx_20210518 limit 1000")
	fmt.Println(rows.Columns())
	return
}

func (sc *shardingDbClient) getDb(index int) (sdb *sql.DB) {
	sdb = dbMap["smcdm_ais2021"]
	return
}

func (sc *shardingDbClient) Query(query string, args ...interface{}) (rows *sql.Rows, err error) {
	sdb := sc.getDb(2021)
	rows, err = sdb.Query(query, args...)
	if err != nil {
		log.Panicf("Error query db: %v", err)
		return
	}
	return
}
