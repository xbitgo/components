package delaybus

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/segmentio/kafka-go"
	"github.com/xbitgo/components/MQ"
	"github.com/xbitgo/core/tools/tool_time"
)

func TestNewRedisDelayer(t *testing.T) {
	mqProducer := MQ.NewKafkaProducer(MQ.KafkaProducerConfig{
		Brokers:  []string{"127.0.0.1:9092"},
		Topic:    "test",
		Balancer: "",
	})

	go func() {
		mqConsumer := MQ.NewKafkaConsumer(kafka.ReaderConfig{
			Brokers:  []string{"127.0.0.1:9092"},
			GroupID:  "test1",
			Topic:    "test",
			MinBytes: 10e3, // 10k
			MaxWait:  time.Second,
			MaxBytes: 10e6, // 10MB
		})
		_ = mqConsumer.Consume(context.Background(), func(message []byte) error {
			fmt.Printf("msg: %s,time:%s \n", message, tool_time.TimeToDateTimeString(time.Now()))
			return nil
		})
	}()

	redisCli := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	bus := NewBus(mqProducer, NewRedisDelayer(redisCli, "test_svc"))
	i := 0
	for range time.Tick(1 * time.Second) {
		err := bus.Add(context.Background(), time.Now().Add(30*time.Second), []byte("test msg"+tool_time.TimeToDateTimeString(time.Now())))
		if err != nil {
			panic(err)
		}
		i++
		if i >= 10 {
			break
		}
	}

	select {}
}
