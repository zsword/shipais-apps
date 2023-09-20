package db

import (
	"fmt"

	"github.com/dhcc/redis-ais/config"

	"github.com/go-redis/redis"
)

type RedisClient struct {
	db *redis.Client
}

var Redisdb *RedisClient

func InitRedis() (err error) {
	props := config.RedisProps
	addr := fmt.Sprintf("%s:%d", props.Host, props.Port)
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "",
		DB:       props.Database,
	})

	_, err = rdb.Ping().Result()
	if err != nil {
		fmt.Println(err)
		return
	}
	client := RedisClient{rdb}
	Redisdb = &client
	fmt.Println(Redisdb.db)
	fmt.Println("DBOK")
	return
}

func (rc *RedisClient) Keys(pattern string) (keys []string, err error) {
	fmt.Print("db: ")
	fmt.Println(rc)
	keys, err = rc.db.Keys(pattern).Result()
	if err != nil {
		fmt.Printf("get keys %s failed, err:%v\n", pattern, err)
		return
	}
	return
}

func (rc *RedisClient) MGet(keys []string) (values []interface{}, err error) {
	values, err = rc.db.MGet(keys...).Result()
	if err != nil {
		fmt.Printf("mget values %d failed, err:%v\n", len(keys), err)
		return
	}
	return
}

func (rc *RedisClient) Del(keys []string) (count int64, err error) {
	if len(keys) < 1 {
		return
	}
	count, err = rc.db.Del(keys...).Result()
	if err != nil {
		fmt.Printf("remove sorted set %d failed, err:%v\n", len(keys), err)
		return
	}
	return
}

func (rc *RedisClient) SMembers(key string) (keys []string, err error) {
	keys, err = rc.db.SMembers(key).Result()
	if err != nil {
		fmt.Printf("list set members %s failed, err:%v\n", key, err)
		return
	}
	return
}

func (rc *RedisClient) ZCard(key string) (count int64, err error) {
	count, err = rc.db.ZCard(key).Result()
	if err != nil {
		fmt.Printf("count sorted set %s failed, err:%v\n", key, err)
		return
	}
	return
}

func (rc *RedisClient) Exists(keys []string) (res int64, err error) {
	res, err = rc.db.Exists(keys...).Result()
	if err != nil {
		fmt.Printf("exists keys %d failed, err:%v\n", len(keys), err)
		return
	}
	return
}

func (rc *RedisClient) ZRem(key string, members []interface{}) (count int64, err error) {
	if len(members) < 1 {
		return
	}
	count, err = rc.db.ZRem(key, members...).Result()
	if err != nil {
		fmt.Printf("remove sorted set %s failed, err:%v\n", key, err)
		return
	}
	return
}

func (rc *RedisClient) ZRange(key string, start int64, stop int64) (keys []string, err error) {
	keys, err = rc.db.ZRange(key, start, stop).Result()
	if err != nil {
		fmt.Printf("range sorted set %s failed, err:%v\n", key, err)
		return
	}
	return
}

func (rc *RedisClient) HKeys(key string) (keys []string, err error) {
	keys, err = rc.db.HKeys(key).Result()
	if err != nil {
		fmt.Printf("list hash keys %s failed, err:%v\n", key, err)
		return
	}
	return
}

func (rc *RedisClient) GeoHash(key string, member []string) (keys []string, err error) {
	keys, err = rc.db.GeoHash(key, member...).Result()
	if err != nil {
		fmt.Printf("list geo hash %s failed, err:%v\n", key, err)
		return
	}
	return
}
