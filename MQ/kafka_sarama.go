package MQ

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"

	"github.com/xbitgo/core/log"
)

type SaramaKafka struct {
	*SaramaKafkaConsumer
	*SaramaKafkaProducer
}

type SaramaKafkaConsumerConfig struct {
	Version   sarama.KafkaVersion
	Brokers   []string
	GroupID   string
	Topic     string
	SaramaCfg sarama.Config
}

type SaramaKafkaConsumer struct {
	cfg    SaramaKafkaConsumerConfig
	reader sarama.ConsumerGroup
}

func (k *SaramaKafkaConsumer) Consume(ctx context.Context, f func(message []byte) error) error {
	for {
		select {
		case <-ctx.Done():
			if err := k.reader.Close(); err != nil {
				log.Fatal("failed to close kafka reader:", err)
			}
		default:
			for {
				topics := []string{k.cfg.Topic}
				handler := consumerHandler{
					f: f,
				}
				err := k.reader.Consume(context.Background(), topics, handler)
				if err != nil {
					log.Errorf("SaramaKafka failed to start reader Consume:", err)
				}
			}
		}
	}
}

type SaramaKafkaProducerConfig struct {
	Brokers   []string `json:"brokers" yaml:"brokers"`
	Topic     string   `json:"topic" yaml:"topic"`
	SaramaCfg *sarama.Config
}

type SaramaKafkaProducer struct {
	cfg     SaramaKafkaProducerConfig
	writer  sarama.AsyncProducer
	closeCh chan struct{}
	closed  bool
}

func (k *SaramaKafkaProducer) Close() {
	k.closed = true
	k.closeCh <- struct{}{}
}

func (k *SaramaKafkaProducer) Produce(ctx context.Context, message []byte, key ...string) error {
	if k.closed {
		return errors.New("SaramaKafkaProducer is closed")
	}
	var _key sarama.Encoder
	if len(key) > 0 {
		_key = sarama.StringEncoder(key[0])
	}
	k.writer.Input() <- &sarama.ProducerMessage{Topic: k.cfg.Topic, Key: _key, Value: sarama.ByteEncoder(message)}
	return nil
}

func NewSaramaKafkaProducer(conf SaramaKafkaProducerConfig) (*SaramaKafkaProducer, error) {
	if conf.SaramaCfg == nil {
		conf.SaramaCfg = sarama.NewConfig()
		conf.SaramaCfg.Version = sarama.V3_1_0_0
	}
	producer, err := sarama.NewAsyncProducer(conf.Brokers, conf.SaramaCfg)
	if err != nil {
		return nil, err
	}
	closeCh := make(chan struct{}, 0)
	p := &SaramaKafkaProducer{
		writer:  producer,
		closeCh: closeCh,
		cfg:     conf,
	}
	go func() {
	ProducerLoop:
		for {
			select {
			case err := <-producer.Errors():
				log.Errorf("SaramaKafkaProducer Failed to write message： %v", err)
			case <-closeCh:
				break ProducerLoop
			}
		}
		err := p.writer.Close()
		if err != nil {
			log.Errorf("SaramaKafkaProducer writer close err： %v", err)
		}
	}()
	return p, nil
}

func NewSaramaKafkaConsumer(cfg SaramaKafkaConsumerConfig) (*SaramaKafkaConsumer, error) {
	config := sarama.NewConfig()
	config.Version = cfg.Version // specify appropriate version
	config.Consumer.Return.Errors = true
	group, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, config)
	if err != nil {
		return nil, err
	}
	go func() {
		for err := range group.Errors() {
			log.Errorf("SaramaKafkaConsumer.Return.Errors  err; %v", err)
		}
	}()
	return &SaramaKafkaConsumer{
		reader: group,
		cfg:    cfg,
	}, nil
}

type consumerHandler struct {
	f func(message []byte) error
}

func (consumerHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (consumerHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (c consumerHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for m := range claim.Messages() {
		log.Infof("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		sess.MarkMessage(m, "")
		if err := c.f(m.Value); err != nil {
			log.Errorf("SaramaKafkaConsumer do func err; %v", err)
		}
	}
	return nil
}
