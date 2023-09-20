package main

import (
	"github.com/dhcc/redis-ais/app/db"
	"github.com/dhcc/redis-ais/config"

	"github.com/gin-gonic/gin"
)

func main() {
	err := config.InitConfig()
	if err != nil {
		return
	}
	if "Release" == config.AppConfig.Mode {
		gin.SetMode(gin.ReleaseMode)
	}
	err = db.InitRedis()
	if err != nil {
		return
	}
	r := gin.Default()
	SetupRouter(r)
	port := config.AppConfig.Port
	if port == "" {
		port = "8088"
	}
	r.Run(":" + port)
}
