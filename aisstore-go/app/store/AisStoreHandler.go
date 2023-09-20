package store

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/dhcc/aisstore-go/app/kafka"
	"github.com/dhcc/aisstore-go/config"
)

type storeHandler interface {
	SetReady()
}

func InitAisStoreHandler(groupId string) (err error) {
	kclient := kafka.Client
	topic := StoreTopics.AisCableTopic
	parts, err := kclient.Partitions(topic)
	fmt.Println(parts)
	com1, cerr := sarama.NewConsumerGroupFromClient(groupId, kclient)
	if cerr != nil {
		err = cerr
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	aisRegion := config.AppConfig.AisInfo.Region
	topics := []string{StoreTopics.AisCableTopic, StoreTopics.AisMarineTopic}
	switch aisRegion {
	case "Land":
		topics = []string{StoreTopics.AisLandTopic}
	}
	handler := (sarama.ConsumerGroupHandler)(nil)
	appconfig := config.AppConfig
	switch appconfig.StoreEngine {
	case StoreEngine.Database:
		handler = &databaseStoreHandler{
			ready:   make(chan bool),
			groupId: groupId,
		}
	case StoreEngine.Elastics:
		handler = &elasticsStoreHandler{
			ready:   make(chan bool),
			groupId: groupId,
		}
	default:
		handler = &redisStoreHandler{
			ready:   make(chan bool),
			groupId: groupId,
		}
	}
	handler.(storeHandler).SetReady()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			err = com1.Consume(ctx, topics, handler)
			if err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			handler.(storeHandler).SetReady()
		}
	}()
	log.Printf("%s consumer started", groupId)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
		log.Println("terminating: context cancelled")
	case <-sigterm:
		log.Println("terminating: via signal")
	}
	cancel()
	wg.Wait()
	if err = com1.Close(); err != nil {
		log.Panicf("Error closing consumer: %v", err)
	}
	return
}
