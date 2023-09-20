package db

import (
	"fmt"

	"github.com/dhcc/aisstore-go/config"
	"github.com/go-redis/redis"
)

type redisClient struct {
	db *redis.Client
}

var Redisdb *redisClient

func InitRedis() (err error) {
	props := config.AppConfig.Redis
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
	client := redisClient{rdb}
	Redisdb = &client
	fmt.Printf("[ok] Init Redis: ")
	fmt.Println(rdb)
	return
}

func (rc *redisClient) Keys(pattern string) (keys []string, err error) {
	fmt.Print("db: ")
	fmt.Println(rc)
	keys, err = rc.db.Keys(pattern).Result()
	if err != nil {
		fmt.Printf("get keys %s failed, err:%v\n", pattern, err)
		return
	}
	return
}

func (rc *redisClient) MGet(keys []string) (values []interface{}, err error) {
	values, err = rc.db.MGet(keys...).Result()
	if err != nil {
		fmt.Printf("mget values %d failed, err:%v\n", len(keys), err)
		return
	}
	return
}

func (rc *redisClient) MSet(pairs []interface{}) (result string, err error) {
	result, err = rc.db.MSet(pairs...).Result()
	if err != nil {
		fmt.Printf("mget values %d failed, err:%v\n", len(result), err)
		return
	}
	return
}

func (rc *redisClient) Del(keys []string) (count int64, err error) {
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

func (rc *redisClient) SMembers(key string) (keys []string, err error) {
	keys, err = rc.db.SMembers(key).Result()
	if err != nil {
		fmt.Printf("list set members %s failed, err:%v\n", key, err)
		return
	}
	return
}

func (rc *redisClient) ZCard(key string) (count int64, err error) {
	count, err = rc.db.ZCard(key).Result()
	if err != nil {
		fmt.Printf("count sorted set %s failed, err:%v\n", key, err)
		return
	}
	return
}

func (rc *redisClient) Exists(keys []string) (res int64, err error) {
	res, err = rc.db.Exists(keys...).Result()
	if err != nil {
		fmt.Printf("exists keys %d failed, err:%v\n", len(keys), err)
		return
	}
	return
}

func (rc *redisClient) ZRem(key string, members []interface{}) (count int64, err error) {
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

func (rc *redisClient) ZRange(key string, start int64, stop int64) (keys []string, err error) {
	keys, err = rc.db.ZRange(key, start, stop).Result()
	if err != nil {
		fmt.Printf("range sorted set %s failed, err:%v\n", key, err)
		return
	}
	return
}

func (rc *redisClient) HKeys(key string) (keys []string, err error) {
	keys, err = rc.db.HKeys(key).Result()
	if err != nil {
		fmt.Printf("list hash keys %s failed, err:%v\n", key, err)
		return
	}
	return
}

func (rc *redisClient) GeoHash(key string, member []string) (keys []string, err error) {
	keys, err = rc.db.GeoHash(key, member...).Result()
	if err != nil {
		fmt.Printf("list geo hash %s failed, err:%v\n", key, err)
		return
	}
	return
}

func (rc *redisClient) GeoAdd(key string, locations []*redis.GeoLocation) (count int64, err error) {
	count, err = rc.db.GeoAdd(key, locations...).Result()
	if err != nil {
		fmt.Printf("add geo hash %d failed, err:%v\n", len(locations), err)
		return
	}
	return
}

func (rc *redisClient) Pipelined(fn func(redis.Pipeliner) error) (cmds []redis.Cmder, err error) {
	cmds, err = rc.db.Pipelined(fn)
	if err != nil {
		fmt.Printf("pipelined error %d failed, err:%v\n", len(cmds), err)
		return
	}
	return
}

func (rc *redisClient) Pipeline() (pipeliner redis.Pipeliner) {
	pipeliner = rc.db.Pipeline()
	return
}

func (rc *redisClient) TxPipeline() (pipeliner redis.Pipeliner) {
	pipeliner = rc.db.TxPipeline()
	return
}
