package main

import (
	"context"
	"fmt"
	"github.com/xbitgo/components/MQ"
	"time"
)

func main() {
	//produce2()
	produce1()

	select {
	case <-time.After(10 * time.Second):

	}

}

func produce1()  {
	producer, err := MQ.NewSaramaKafkaProducer(MQ.SaramaKafkaProducerConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test",
	})
	if err != nil {
		panic(err)
	}
	for i := 0; i < 1000; i++ {
		err := producer.Produce(context.Background(), []byte("Sarama-xxxxxxx"))
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("producer ok")
}

func produce2()  {
	producer := MQ.NewKafkaProducer(MQ.KafkaProducerConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "test",
		Async: true,
	})
	for i := 0; i < 10; i++ {
		err := producer.Produce(context.Background(), []byte("kafka-go-xxxxx"))
		fmt.Println("xxxx")
		if err != nil {
			panic(err)
		}
	}
}