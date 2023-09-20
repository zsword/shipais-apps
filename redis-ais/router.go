package main

import (
	"net/http"

	"github.com/dhcc/redis-ais/app/controller"
	"github.com/dhcc/redis-ais/app/routes"
	"github.com/gin-gonic/gin"
)

func SetupRouter(router *gin.Engine) *gin.Engine {
	router.GET("/hello", controller.SayHello)
	router.LoadHTMLGlob("./html/*")
	router.GET("/", func(c *gin.Context) {
		c.HTML(http.StatusOK, "index.html", nil)
	})
	routes.AisDataRouter.InitRouter(router)
	router.Static("/public", "./public")
	return router
}
