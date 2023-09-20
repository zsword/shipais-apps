package db

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"time"

	"github.com/dhcc/aisstore-go/app/model"
	"github.com/dhcc/aisstore-go/config"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type shardingOrmClient struct {
}

var (
	ormDbMap          map[string]*gorm.DB
	ShardingOrmClient shardingOrmClient
)

func InitOrmDB() (err error) {
	props := config.AppConfig.Database
	dbLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
		logger.Config{
			SlowThreshold:             time.Second,   // Slow SQL threshold
			LogLevel:                  logger.Silent, // Log level
			IgnoreRecordNotFoundError: true,          // Ignore ErrRecordNotFound error for logger
			Colorful:                  false,         // Disable color
		},
	)
	ormDbMap = map[string]*gorm.DB{}
	for y := 2021; y < 2024; y++ {
		dbKey := fmt.Sprintf("%s%d", props.Dbname, y)
		dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable TimeZone=Asia/Shanghai", props.Host, props.Port, props.User, props.Password, dbKey)
		config := gorm.Config{
			Logger: dbLogger,
		}
		ormDb, oerr := gorm.Open(postgres.Open(dsn), &config)
		if oerr != nil {
			log.Panicf("Error init orm db: %v", oerr)
			err = oerr
			return
		}
		//ormDb.Callback().Create().Register("sharding", shardingTable)
		ormDbMap[dbKey] = ormDb
	}
	ShardingOrmClient = shardingOrmClient{}
	fmt.Printf("[ok] Init Database: %d\n", len(ormDbMap))
	return
}

func (c *shardingOrmClient) SaveInBatches(dataKey string, list interface{}) error {
	dbKey := dataKey[0 : len(dataKey)-4]
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
	dbKey := dataKey[0 : len(dataKey)-4]
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

func (c *shardingOrmClient) Save(dataKey string, data interface{}) (err error) {
	dbKey := dataKey[0 : len(dataKey)-4]
	ormdb := ormDbMap[dbKey]
	table := ormdb.Scopes(shipAisTable(data))
	err = table.Create(data).Error
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

func shardingTable(db *gorm.DB) {
	if db.Statement.Schema != nil {
		// crop image fields and upload them to CDN, dummy code
		for _, field := range db.Statement.Schema.Fields {
			switch db.Statement.ReflectValue.Kind() {
			case reflect.Slice, reflect.Array:
				for i := 0; i < db.Statement.ReflectValue.Len(); i++ {
					// Get value from field
					if fieldValue, isZero := field.ValueOf(db.Statement.ReflectValue.Index(i)); !isZero {
						if fieldValue != nil {
							//fmt.Println(fieldValue)
						}
					}
				}
			case reflect.Struct:
				// Get value from field
				if fieldValue, isZero := field.ValueOf(db.Statement.ReflectValue); !isZero {
					if fieldValue != nil {
						//fmt.Println(fieldValue)
					}
				}
			}
		}

		// processing
	}
}
