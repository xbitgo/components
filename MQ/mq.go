package MQ

import "context"

type Producer interface {
	Produce(ctx context.Context, message []byte, key ...string) error
}

type Consumer interface {
	Consume(ctx context.Context, f func(message []byte) error) error
}

type Queue interface {
	Producer
	Consumer
}
