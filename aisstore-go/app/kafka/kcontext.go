package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/dhcc/aisstore-go/config"
)

var Client sarama.Client

func InitKafka() (err error) {
	props := config.AppConfig.Kafka
	config := sarama.NewConfig()
	client, cerr := sarama.NewClient(props.Addrs, config)
	if cerr != nil {
		fmt.Printf("[ERROR] %s", cerr)
		err = cerr
		return
	}
	Client = client
	fmt.Printf("[ok] Init Kafka: %s\n", client.Config().Version)
	return
}
