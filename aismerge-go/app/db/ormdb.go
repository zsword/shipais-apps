package db

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/dhcc/aismerge-go/app/logs"
	"github.com/dhcc/aismerge-go/app/model"
	"github.com/dhcc/aismerge-go/config"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type shardingOrmClient struct {
}

var (
	ormDbMap          map[string]*gorm.DB
	defaultDbKey      string
	ShardingOrmClient shardingOrmClient
)

func InitOrmDB() (err error) {
	props := config.AppConfig.Database
	dbLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold:             time.Second, // Slow SQL threshold
			LogLevel:                  logger.Warn, // Log level
			IgnoreRecordNotFoundError: true,        // Ignore ErrRecordNotFound error for logger
			Colorful:                  false,       // Disable color
		},
	)
	ormDbMap = map[string]*gorm.DB{}
	for y := 2021; y < 2024; y++ {
		dbKey := fmt.Sprintf("%s%d", props.Dbname, y)
		if defaultDbKey == "" {
			defaultDbKey = dbKey
		}
		dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable TimeZone=Asia/Shanghai", props.Host, props.Port, props.User, props.Password, dbKey)
		config := gorm.Config{
			Logger: dbLogger,
		}
		ormDb, oerr := gorm.Open(postgres.Open(dsn), &config)
		//dsn = "root:root@tcp(127.0.0.1:3306)/smcdm_ais?charset=utf8&parseTime=True&loc=Asia%2FShanghai"
		//ormDb, oerr := gorm.Open(mysql.Open(dsn), &config)
		if oerr != nil {
			log.Panicf("Error init orm db: %v", oerr)
			err = oerr
			return
		}
		ormDbMap[dbKey] = ormDb
	}
	ShardingOrmClient = shardingOrmClient{}
	fmt.Printf("[ok] Init Database: %d\n", len(ormDbMap))
	return
}

func buildDbKey(dataKey string) string {
	return dataKey[0:13]
}

func (c *shardingOrmClient) SaveInBatches(dataKey string, list interface{}) error {
	dbKey := buildDbKey(dataKey)
	ormdb := ormDbMap[dbKey]
	table := ormdb.Scopes(shipAisTable(list))
	err := table.CreateInBatches(list, 100).Error
	//err := ormdb.CreateInBatches(list, 100).Error
	if err != nil {
		return err
	}
	return nil
}

func (c *shardingOrmClient) SaveAll(dataKey string, list interface{}) (res []int) {
	dbKey := buildDbKey(dataKey)
	count := 0
	errcount := 0
	ormdb := ormDbMap[dbKey]
	table := ormdb.Scopes(shipAisTable(list))
	values := reflect.ValueOf(list)
	for i := 0; i < values.Len(); i++ {
		v := values.Index(i).Interface()
		err := table.Create(v).Error
		if err != nil {
			errcount++
			continue
		}
		count++
	}
	res = []int{count, errcount}
	return
}

func (c *shardingOrmClient) Save(data interface{}) (err error) {
	dbKey := defaultDbKey
	ormdb := ormDbMap[dbKey]
	err = ormdb.Save(data).Error
	if err != nil {
		fmt.Println(err)
	}
	return
}

func (c *shardingOrmClient) SaveSharding(dataKey string, data interface{}) (err error) {
	dbKey := buildDbKey(dataKey)
	ormdb := ormDbMap[dbKey]
	table := ormdb.Scopes(shipAisTable(data))
	err = table.Save(data).Error
	return
}

func shipAisTable(list interface{}) func(tx *gorm.DB) *gorm.DB {
	tableName := config.AIS_TABLENAME
	data := list
	valueType := reflect.ValueOf(list)
	switch valueType.Kind() {
	case reflect.Slice, reflect.Array:
		if valueType.Len() < 1 {
			data = nil
		} else {
			data = valueType.Index(0).Interface()
		}
	default:
	}
	if data != nil {
		tableName = data.(model.ShipAisInfo).TableName()
	}
	return func(tx *gorm.DB) *gorm.DB {
		return tx.Table(tableName)
	}
}

func (c *shardingOrmClient) FindSharding(dataKey string, params map[string]interface{}, list interface{}) (err error) {
	dbKey := buildDbKey(dataKey)
	ormdb := ormDbMap[dbKey]
	query, args, tbnames := c.buildQuery(params)
	tblist := strings.Split(tbnames, ",")
	for _, tb := range tblist {
		if tb == "" {
			continue
		}
		table := ormdb.Table(tb)
		err = table.Error
		if err != nil {
			fmt.Println(err)
			return
		}
		err = table.Where(query, args...).Find(list).Error
		if err != nil {
			fmt.Println(err)
			return
		}
	}
	return
}

func (c *shardingOrmClient) FindOne(dest interface{}) (err error) {
	return c.Find(dest, nil, 0, 1)
}

func (c *shardingOrmClient) Find(dest interface{}, order interface{}, offset int, limit int) (err error) {
	ormdb := ormDbMap[defaultDbKey]
	db := ormdb.Where(dest)
	if order != nil {
		db = db.Order(order)
	}
	if offset > -1 {
		db = db.Offset(offset)
	}
	if limit > 0 {
		db = db.Limit(limit)
	}
	err = db.Find(&dest).Error
	if err != nil {
		logs.Error("Find data: %s", err)
	}
	return
}

func (c *shardingOrmClient) buildQuery(params map[string]interface{}) (query string, args []interface{}, tbnames string) {
	hourMS := int64(time.Hour / time.Millisecond)
	for key, val := range params {
		if len(query) < 1 {
			query += key
		} else {
			query += " AND " + key
		}
		isSharding := strings.Contains(key, "jssjc")
		valType := reflect.ValueOf(val)
		switch valType.Kind() {
		case reflect.Slice, reflect.Array:
			for n := 0; n < valType.Len(); n++ {
				v := valType.Index(n).Interface()
				args = append(args, v)
				if isSharding {
					vt := v.(time.Time)
					if n > 0 {
						if vt.UnixMilli()%hourMS == 0 {
							continue
						}
					}
					tb := model.BuildAisTableName(v.(time.Time))
					if !strings.Contains(tbnames, tb) {
						tbnames += tb + ","
					}
				}
			}
		default:
			args = append(args, val)
			if isSharding {
				tb := model.BuildAisTableName(val.(time.Time))
				if !strings.Contains(tbnames, tb) {
					tbnames += tb + ","
				}
			}
		}
	}
	return
}

func (c *shardingOrmClient) Count(dataKey string, params map[string]interface{}) (total int64, err error) {
	dbKey := buildDbKey(dataKey)
	ormdb := ormDbMap[dbKey]
	tbnames := ""
	query, args, tbnames := c.buildQuery(params)
	tblist := strings.Split(tbnames, ",")
	for _, tb := range tblist {
		if tb == "" {
			continue
		}
		table := ormdb.Table(tb)
		err = table.Error
		if err != nil {
			fmt.Println(err)
			return
		}
		count := int64(0)
		tbq := table.Where(query, args...).Count(&count)
		err = tbq.Error
		if err != nil {
			fmt.Println(err)
			return
		}
		total += count
	}
	return
}

func (c shardingOrmClient) ExecuteSQL(sql string) (err error) {
	ormdb := ormDbMap[defaultDbKey]
	db, err := ormdb.DB()
	rows, err := db.Query(sql)
	if err != nil {
		fmt.Println(err)
	}
	for rows.Next() {
		fmt.Println(rows)
	}
	return
}
