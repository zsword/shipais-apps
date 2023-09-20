package controller

import (
	"github.com/gin-gonic/gin"
)

func Register(c *gin.Context) {
	//返回结果
	c.JSON(200, gin.H{
		"msg": "msg received",
	})
}
