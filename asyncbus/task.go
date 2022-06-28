package asyncbus

import (
	"github.com/xbitgo/components/MQ"
	"time"
)

type Task interface {
	TaskName() string
}

// OnceTask 单次任务
type OnceTask interface {
	At() time.Time
	Run() error
	Task
}

// CronTask 定时任务
type CronTask interface {
	CronSpec() string
	CronJob() error
	Task
}

// ConsumeTask 消费任务
type ConsumeTask interface {
	MQConsumer() MQ.Consumer
	ConsumeJob(buf []byte) error
	Task
}
