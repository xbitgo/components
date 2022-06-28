package delaybus

import (
	"context"
	"github.com/xbitgo/core/log"
	"time"

	"github.com/xbitgo/components/MQ"
)

type Delayer interface {
	Add(ctx context.Context, at time.Time, message []byte, key ...string) error
	Consume() chan []byte
}

type delayMsg struct {
	Key string `json:"key"`
	Msg []byte `json:"msg"`
}

type Bus struct {
	producer MQ.Producer
	delayer  Delayer
}

func NewBus(producer MQ.Producer, delayer Delayer) *Bus {
	bus := &Bus{
		producer: producer,
		delayer:  delayer,
	}
	go bus._send()
	return bus
}

func (b *Bus) Add(ctx context.Context, at time.Time, message []byte, key ...string) error {
	return b.delayer.Add(ctx, at, message, key...)
}

func (b *Bus) _send() {
	for message := range b.delayer.Consume() {
		err := b.producer.Produce(context.Background(), message)
		if err != nil {
			log.Errorf("delaybus err: %v", err)
		}
	}
}
