package db

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/dhcc/aisstore-go/app/logs"
	"github.com/dhcc/aisstore-go/app/model"
	"github.com/dhcc/aisstore-go/config"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
)

type elasticsClient struct {
	db *elasticsearch.Client
}
type bulkResponse struct {
	Errors bool `json:"errors"`
	Items  []struct {
		Index struct {
			ID     string `json:"_id"`
			Result string `json:"result"`
			Status int    `json:"status"`
			Error  struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
				Cause  struct {
					Type   string `json:"type"`
					Reason string `json:"reason"`
				} `json:"caused_by"`
			} `json:"error"`
		} `json:"index"`
	} `json:"items"`
}

var Elasticsdb *elasticsClient

func InitElastics() (err error) {
	props := config.AppConfig.Elastics

	config := elasticsearch.Config{
		Addresses: props.Addresses,
		Username:  props.User,
		Password:  props.Password,
		CloudID:   props.CloudID,
		APIKey:    props.ApiKey,
	}
	client, err := elasticsearch.NewClient(config)
	if err != nil {
		fmt.Println(err)
		return
	}
	res, err := client.Info()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(res)
	defer res.Body.Close()
	Elasticsdb = &elasticsClient{client}
	fmt.Printf("[ok] Init Elasticsdb: ")
	fmt.Println(client)
	return
}

func (ec *elasticsClient) CreateIndex(name string, mapping string) (err error) {
	db := ec.db

	resp, err := db.Indices.Exists([]string{name})
	if err != nil {
		fmt.Println(err)
		return
	}
	if resp.StatusCode == 200 {
		fmt.Printf("[ok] Init data indices: %s\n", name)
		return
	}
	resp, err = db.Indices.Create(name, db.Indices.Create.WithBody(strings.NewReader(mapping)))
	if err != nil {
		fmt.Println(err)
	}
	if resp.StatusCode != 200 {
		fmt.Println(resp)
		err = fmt.Errorf("[err] Init data indexces")
		return
	}
	fmt.Printf("[ok] Init data indices: ")
	fmt.Println(resp)
	return
}

func (ec *elasticsClient) Save(index string, data model.IdData) (sid string, err error) {
	db := ec.db
	payload, err := json.Marshal(data)
	id := data.GetId()
	if err != nil {
		logs.Warn("Error encode data: %s, %s", id, err)
		return
	}

	resp, err := db.Exists(index, id)
	if err != nil {
		logs.Warn("Exists index: %s, %s", index, err)
		return
	}
	if resp.StatusCode == 200 {
		resp, err = db.Update(index, id, bytes.NewReader(payload))
		return
	} else {
		resp, err = db.Create(index, id, bytes.NewReader(payload))
	}
	if err != nil {
		logs.Error("Save index: %s, %s, %s", index, id, err)
		return
	}
	fmt.Println(resp)
	return
}

const batch = 1000

func (ec *elasticsClient) SaveAll(index string, list interface{}) (saves uint32, err error) {
	db := ec.db

	listType := reflect.Indirect(reflect.ValueOf(list))
	count := listType.Len()
	if count < 1 {
		return
	}
	var (
		buf  bytes.Buffer
		resp *esapi.Response
		bulk *bulkResponse
	)
	data := listType.Index(0).Interface()
	dataType := reflect.TypeOf(data)
	isMap := strings.Contains(dataType.String(), "map[")
	numErrors := uint32(0)
	numSaves := uint32(0)
	for i := 0; i < count; i++ {
		item := listType.Index(i).Interface()
		id := ""
		if isMap {
			mval := item.(map[string]interface{})
			id = mval["_id"].(string)
			delete(mval, "_id")
		} else {
			id = item.(model.IdData).GetId()
		}

		meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%s" } }%s`, id, "\n"))
		data, merr := json.Marshal(item)
		if merr != nil {
			logs.Warn("Encode data %s: %s", id, merr)
			continue
		}
		data = append(data, "\n"...)

		buf.Grow(len(meta) + len(data))
		buf.Write(meta)
		buf.Write(data)

		if i > 0 && i%batch == 0 || i == count-1 {
			resp, err = db.Bulk(bytes.NewReader(buf.Bytes()), db.Bulk.WithIndex(index))
			if err != nil {
				logs.Error("Save index batch %d: %s", i, err)
				return
			}
			if resp.IsError() {
				logs.Error("Save index batch %d: %s", i, resp.String())
				return
			}
			err := json.NewDecoder(resp.Body).Decode(&bulk)
			if err != nil {
				logs.Error("Parse response body: %s", err)
			}
			for _, d := range bulk.Items {
				if d.Index.Status > 201 {
					numErrors++
					logs.Warn("Save data: [%d]: %s: %s: %s: %s",
						d.Index.Status,
						d.Index.Error.Type,
						d.Index.Error.Reason,
						d.Index.Error.Cause.Type,
						d.Index.Error.Cause.Reason,
					)
				} else {
					numSaves++
				}
			}
			resp.Body.Close()
			buf.Reset()
		}
	}
	saves = numSaves
	return
}
