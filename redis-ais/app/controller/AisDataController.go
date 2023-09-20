package controller

import (
	"net/http"

	"github.com/dhcc/redis-ais/app/model"
	"github.com/dhcc/redis-ais/app/service"
	"github.com/gin-gonic/gin"
)

type aisDataApi struct {
}

var AisDataApi *aisDataApi

func init() {
	AisDataApi = &aisDataApi{}
}

func (ac *aisDataApi) CountByTimes(c *gin.Context) {
	svc := service.AisDataService
	cmap, err := svc.CountByTimes()
	result := model.NewResult(200, nil, "")
	if err != nil {
		result.SetError("按时间计数出错: ", err)
		c.JSON(http.StatusOK, result)
		return
	}
	geoSize, err := svc.CountDetachedGeo()
	if err != nil {
		result.SetError("按时间计数出错: ", err)
		c.JSON(http.StatusOK, result)
		return
	}
	cmap["allGeo"] = geoSize[0]
	cmap["_geo"] = geoSize[1]
	result.SetData(cmap)
	c.JSON(http.StatusOK, result)
}

func (ac *aisDataApi) CleanData(c *gin.Context) {
	cleanType := c.PostForm("type")
	if cleanType == "" {
		cleanType, _ = c.GetQuery("type")
	}
	result := model.NewResult(200, nil, "")
	if cleanType == "" {
		result.SetError("缺少清理类型参数", nil)
		c.JSON(http.StatusOK, result)
		return
	}
	svc := service.AisDataService
	res, err := svc.CleanData(cleanType)
	if err != nil {
		result.SetError("清理数据出错: ", err)
		c.JSON(http.StatusOK, result)
		return
	}
	result.SetData(res)
	c.JSON(http.StatusOK, result)
}
