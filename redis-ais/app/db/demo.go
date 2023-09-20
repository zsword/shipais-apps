package db

import (
	"fmt"

	"github.com/dhcc/redis-ais/config"
)

func SayDemo() (err error) {
	fmt.Println(config.RedisProps.Host)
	return
}
