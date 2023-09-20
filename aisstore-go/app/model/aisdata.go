package model

import (
	"context"
	"fmt"
	"time"

	"github.com/dhcc/aisstore-go/config"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Location struct {
	X, Y float64
}

func (loc Location) GormDataType() string {
	return "geometry"
}

func (loc Location) GormValue(ctx context.Context, db *gorm.DB) clause.Expr {
	return clause.Expr{
		SQL:  "ST_PointFromText(?, " + config.AIS_GEOSRID + ")",
		Vars: []interface{}{fmt.Sprintf("POINT(%f %f)", loc.X, loc.Y)},
	}
}

// Scan implements the sql.Scanner interface
func (loc *Location) Scan(v interface{}) error {
	// Scan a value into struct from database driver
	return nil
}

type ShipAisInfo struct {
	Mmsi        string    `gorm:"primaryKey" json:"mmsi"`
	Jssjc       time.Time `gorm:"primaryKey"`
	Recivetime  int64     `gorm:"-" json:"receivetime,omitempty,string"`
	Jd          float64   `json:"longitude,omitempty,string"`
	Wd          float64   `json:"latitude,omitempty,string"`
	Zxl         int32     `json:"rot,omitempty,string"`
	Hs          int32     `json:"sog,omitempty,string"`
	Ddhx        int32     `json:"cog,omitempty,string"`
	Zsx         int32     `json:"thead,omitempty,string"`
	Cbhhwlx     string    `json:"shipAndCargType,omitempty"`
	Mc          string    `json:"shipname,omitempty"`
	Utctime     int32     `json:"utctime,omitempty,string"`
	Cl          float32   `json:"length,omitempty,string"`
	Ck          float32   `json:"width,omitempty,string"`
	Mqzdjtcs    float32   `json:"draft,string"`
	Xxlx        string    `json:"type,omitempty"`
	Zfzsf       string    `json:"forward,omitempty"`
	Dhzt        string    `json:"navistat,omitempty"`
	Wzzqd       string    `json:"posacur,omitempty"`
	Tdczzsf     string    `json:"indicator,omitempty"`
	Raim        string    `json:"raim,omitempty"`
	Devicemark  string    `json:"devicemark,omitempty"`
	Dscmark     string    `json:"dscmark,omitempty"`
	Bandmark    string    `json:"bandmark,omitempty"`
	Msg22mark   string    `json:"msg22mark,omitempty"`
	Patternmark string    `json:"patternmark,omitempty"`
	Gnss        string    `json:"gnss,omitempty"`
	Ver         string    `json:"ver,omitempty"`
	Imo         string    `json:"imo,omitempty"`
	Hh          string    `json:"callno,omitempty"`
	Dzdwzzlx    string    `json:"devicetype,omitempty"`
	Dispmark    string    `json:"dispmark,omitempty"`
	Yjdgsj      string    `json:"eta,omitempty"`
	Mdg         string    `json:"dest,omitempty"`
	Dte         string    `json:"dte,omitempty"`
	Geom        Location
	Id          int64
	Sdmptime    time.Time
}

func (info *ShipAisInfo) BuildInfo() {
	info.Sdmptime = time.Now()
	info.Jssjc = time.UnixMilli(info.Recivetime)
	info.Geom = Location{info.Jd, info.Wd}
}

func (info ShipAisInfo) TableName() string {
	return fmt.Sprintf("%s_%s", config.AIS_TABLENAME, info.Jssjc.Format("20060102"))
}

func (info ShipAisInfo) ShardingKey() string {
	return fmt.Sprintf("%s%s", config.AIS_DBKEY, info.Jssjc.Format("20060102"))
}
