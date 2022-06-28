package MQ

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/xbitgo/core/log"
	"github.com/xbitgo/core/tools/tool_str"
)

type Kafka struct {
	*KafkaConsumer
	*KafkaProducer
}

type KafkaConsumer struct {
	reader *kafka.Reader
}

func (k *KafkaConsumer) Consume(ctx context.Context, f func(message []byte) error) error {
	for {
		select {
		case <-ctx.Done():
			if err := k.reader.Close(); err != nil {
				log.Fatal("failed to close kafka reader:", err)
			}
		default:
			m, err := k.reader.ReadMessage(context.Background())
			if err != nil {
				log.Errorf("KafkaConsumer ReadMessage err: %v", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}
			log.Infof("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
			err = f(m.Value)
			if err != nil {
				log.Errorf("KafkaConsumer do func err; %v", err)
			}
		}
	}
}

type KafkaProducerConfig struct {
	Brokers                []string      `json:"brokers" yaml:"brokers"`
	Topic                  string        `json:"topic" yaml:"topic"`
	Balancer               string        `json:"balancer" yaml:"balancer"`
	Async                  bool          `json:"async" yaml:"async"`
	RequiredAcks           string        `json:"required_acks" yaml:"required_acks"`
	AllowAutoTopicCreation bool          `json:"allow_auto_topic_creation" yaml:"allow_auto_topic_creation"`
	MaxAttempts            int           `json:"max_attempts" yaml:"max_attempts"`
	BatchSize              int           `json:"batch_size" yaml:"batch_size"`
	BatchBytes             int64         `json:"batch_bytes" yaml:"batch_bytes"`
	BatchTimeout           time.Duration `json:"batch_timeout" yaml:"batch_timeout"`
	ReadTimeout            time.Duration `json:"read_timeout" yaml:"read_timeout"`
	WriteTimeout           time.Duration `json:"write_timeout" yaml:"write_timeout"`
}

type KafkaProducer struct {
	writer *kafka.Writer
}

func (k *KafkaProducer) Produce(ctx context.Context, message []byte, key ...string) error {
	var _key string
	if len(key) > 0 {
		_key = key[0]
	} else {
		_key = tool_str.UUID()
	}
	err := k.writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(_key),
			Value: message,
		},
	)
	if err != nil {
		log.Errorf("failed to write messages:", err)
	}
	return err
}

func NewKafkaProducer(conf KafkaProducerConfig) *KafkaProducer {
	var balancer kafka.Balancer
	switch conf.Balancer {
	case "hash":
		balancer = &kafka.Hash{}
	case "crc32":
		balancer = &kafka.CRC32Balancer{}
	case "murmur2":
		balancer = &kafka.Murmur2Balancer{}
	case "roundRobin":
		balancer = &kafka.RoundRobin{}
	default:
		balancer = &kafka.LeastBytes{}
	}
	var requiredAcks kafka.RequiredAcks
	switch conf.RequiredAcks {
	case "none":
		requiredAcks = kafka.RequireNone
	case "one":
		requiredAcks = kafka.RequireOne
	case "all":
		requiredAcks = kafka.RequireAll
	default:
		requiredAcks = kafka.RequireNone
	}
	var (
		MaxAttempts        = 10
		BatchSize          = 100
		BatchBytes   int64 = 1048576
		BatchTimeout       = 1 * time.Second
		ReadTimeout        = 10 * time.Second
		WriteTimeout       = 10 * time.Second
	)
	if conf.MaxAttempts > 0 {
		MaxAttempts = conf.MaxAttempts
	}
	if conf.BatchSize > 0 {
		BatchSize = conf.BatchSize
	}
	if conf.BatchBytes > 0 {
		BatchBytes = conf.BatchBytes
	}
	if conf.BatchTimeout > 0 {
		BatchTimeout = conf.BatchTimeout
	}
	if conf.ReadTimeout > 0 {
		ReadTimeout = conf.ReadTimeout
	}
	if conf.WriteTimeout > 0 {
		WriteTimeout = conf.WriteTimeout
	}
	w := &kafka.Writer{
		Addr:                   kafka.TCP(conf.Brokers...),
		Topic:                  conf.Topic,
		Balancer:               balancer,
		Async:                  conf.Async,
		AllowAutoTopicCreation: conf.AllowAutoTopicCreation,
		MaxAttempts:            MaxAttempts,
		BatchSize:              BatchSize,
		BatchBytes:             BatchBytes,
		BatchTimeout:           BatchTimeout,
		ReadTimeout:            ReadTimeout,
		WriteTimeout:           WriteTimeout,
		RequiredAcks:           requiredAcks,
	}
	return &KafkaProducer{writer: w}
}

func NewKafkaConsumer(conf kafka.ReaderConfig) *KafkaConsumer {
	r := kafka.NewReader(conf)
	return &KafkaConsumer{reader: r}
}
