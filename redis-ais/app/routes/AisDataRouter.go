package routes

import (
	"github.com/dhcc/redis-ais/app/controller"
	"github.com/gin-gonic/gin"
)

type webRouter struct {
}

var AisDataRouter *webRouter

func init() {
	AisDataRouter = &webRouter{}
}

func (ar *webRouter) InitRouter(router *gin.Engine) {
	group := router.Group("/ais")
	{
		group.GET("/countByTimes", controller.AisDataApi.CountByTimes)
		group.POST("/cleanData", controller.AisDataApi.CleanData)
	}
}
