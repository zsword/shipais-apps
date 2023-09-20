package service

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/dhcc/redis-ais/app/db"
)

type aisDataService struct {
}

var AisDataService *aisDataService

func init() {
	AisDataService = &aisDataService{}
}

const dayMS = 24 * 3600 * 1000
const _GeoKey = "ais-geo"

func (as *aisDataService) getDb() (rdb *db.RedisClient) {
	rdb = db.Redisdb
	return
}

func (as *aisDataService) CountByTimes() (result map[string]int64, err error) {
	result, err = as.execLoopTimeKeys(as.countTimeKeyFn, -1)
	return
}

func (as *aisDataService) countTimeKeyFn(keys []string, cleanDay int) (res map[string]int64, err error) {
	rdb := as.getDb()
	res, err = as.countTimes(keys, cleanDay)
	if err != nil {
		return
	}
	dkeys := []string{}
	for i := range keys {
		key := keys[i]
		key = strings.Replace(key, "t_", "", 1)
		dkeys = append(dkeys, key)
	}
	values, merr := rdb.MGet(dkeys)
	if merr != nil {
		err = merr
		return
	}
	for i := range values {
		if values[i] == nil {
			res["_t"] += 1
		}
	}
	return
}

func (as *aisDataService) execLoopTimeKeys(fn func([]string, int) (map[string]int64, error), cleanDay int) (result map[string]int64, err error) {
	rdb := db.Redisdb
	tkeys, err := rdb.Keys("t_*")
	if err != nil {
		return
	}
	total := len(tkeys)
	size := 10000
	end := total
	if end > size {
		end = size
	}
	result = map[string]int64{
		"allT": int64(total),
		"dx":   0,
		"_t":   0,
	}
	for d := 0; d < 8; d++ {
		dk := fmt.Sprintf("d%d", d)
		result[dk] = 0
	}
	for i := 0; i < total; {
		keys := tkeys[i:end]
		cmap, ferr := fn(keys, cleanDay)
		if ferr != nil {
			err = ferr
			return
		}
		for dk := range cmap {
			result[dk] = result[dk] + cmap[dk]
		}
		end = end + size
		i += len(keys)
		if i >= total {
			break
		}
		end = i + size
		if end > total {
			end = total
		}
	}
	return
}

func (as *aisDataService) CountDetachedGeo() (res []int64, err error) {
	res, err = as.execLoopGeoData(as.countGeoFn)
	return
}

func (as *aisDataService) countGeoFn(dataKey string, start int64, end int64) (count int64, err error) {
	rdb := as.getDb()
	geoKeys, zerr := rdb.ZRange(dataKey, start, end)
	if zerr != nil {
		err = zerr
		fmt.Println(zerr)
		return
	}
	values, merr := rdb.MGet(geoKeys)
	if merr != nil {
		err = merr
		fmt.Println(merr)
		return
	}
	for n := range values {
		if values[n] == nil {
			count++
		}
	}
	return
}

func (as *aisDataService) countTimes(keys []string, cleanDay int) (result map[string]int64, err error) {
	rdb := as.getDb()
	times, merr := rdb.MGet(keys)
	if merr != nil {
		err = merr
		return
	}
	result = map[string]int64{}
	now := time.Now().Local().UnixMilli()
	todayMS := now % dayMS
	rmkeys := []string{}
	for i := range times {
		tv := times[i]
		if tv == nil {
			continue
		}
		val, perr := strconv.ParseInt(times[i].(string), 10, 64)
		if perr != nil {
			err = perr
			return
		}
		sub := now - val
		dkey := "d0"
		if sub < todayMS {
		} else {
			sub -= todayMS
			idx := sub / dayMS
			if cleanDay > 0 {
				if idx >= int64(cleanDay) {
					rmkeys = append(rmkeys, keys[i])
				}
				continue
			}
			if idx > 7 {
				dkey = "dx"
			} else {
				dkey = fmt.Sprintf("d%d", idx)
			}
		}
		result[dkey] = result[dkey] + 1
	}
	if cleanDay > 0 {
		result["dx"], err = rdb.Del(rmkeys)
	}
	return
}

func (as *aisDataService) CleanData(cleanType string) (res []int64, err error) {
	switch cleanType {
	case "_geo":
		res, err = as.cleanDetachedGeo()
	case "_t":
		res, err = as.cleanDetachedTimeKey()
	case "dx":
		res, err = as.cleanByTimes()
	}
	return
}

func (as *aisDataService) cleanGeoFunc(dataKey string, start int64, end int64) (count int64, err error) {
	rdb := as.getDb()
	count = int64(0)
	oldkeys := []string{}
	geoKeys, zerr := rdb.ZRange(dataKey, start, end)
	if zerr != nil {
		err = zerr
		return
	}
	values, merr := rdb.MGet(geoKeys)
	if merr != nil {
		err = merr
		return
	}
	for n := range values {
		if values[n] == nil {
			oldkeys = append(oldkeys, geoKeys[n])
		}
	}
	rmkeys := make([]interface{}, len(oldkeys))
	for n, v := range oldkeys {
		rmkeys[n] = v
	}
	count, err = rdb.ZRem(dataKey, rmkeys)
	if err != nil {
		return
	}
	return
}

func (as *aisDataService) execLoopGeoData(fn func(string, int64, int64) (int64, error)) (res []int64, err error) {
	dataKey := _GeoKey
	count := int64(0)
	rdb := db.Redisdb
	total, zerr := rdb.ZCard(dataKey)
	if zerr != nil {
		err = zerr
		return
	}
	size := int64(10000)
	end := size
	if end < size {
		end = size
	}
	for i := int64(0); i < total; {
		rc, ferr := fn(dataKey, i, end)
		if ferr != nil {
			err = ferr
			return
		}
		count += rc
		i = end
		if i >= total {
			break
		}
		end += size
		if end > total {
			end = total
		}
	}
	res = []int64{total, count}
	fmt.Println(res)
	return
}

func (as *aisDataService) cleanDetachedGeo() (res []int64, err error) {
	res, err = as.execLoopGeoData(as.cleanGeoFunc)
	return
}

func (as *aisDataService) cleanDetachedTimeKey() (res []int64, err error) {
	cmap, lerr := as.execLoopTimeKeys(as.cleanTimeKeyFn, -1)
	if lerr != nil {
		err = lerr
	}
	res = []int64{cmap["allT"], cmap["_t"]}
	return
}

func (as *aisDataService) cleanTimeKeyFn(keys []string, not int) (res map[string]int64, err error) {
	rdb := as.getDb()
	cmap := map[string]int64{}
	cmap["_t"] = 0
	dkeys := []string{}
	for i := range keys {
		key := keys[i]
		dkeys = append(dkeys, strings.Replace(key, "t_", "", 1))
	}
	values, merr := rdb.MGet(dkeys)
	if merr != nil {
		err = merr
		return
	}
	rmkeys := []string{}
	for i := range values {
		if values[i] == nil {
			rmkeys = append(rmkeys, keys[i])
		}
	}
	cmap["_t"], err = rdb.Del(rmkeys)
	return
}

func (as *aisDataService) cleanByTimes() (res []int64, err error) {
	cmap, lerr := as.execLoopTimeKeys(as.countTimes, 7)
	if lerr != nil {
		err = lerr
		return
	}
	res = []int64{cmap["allT"], cmap["dx"]}
	return
}
