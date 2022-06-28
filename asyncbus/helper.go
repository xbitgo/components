package asyncbus

import (
	"github.com/panjf2000/ants/v2"
	"github.com/xbitgo/components/MQ"
	"github.com/xbitgo/core/di"
	"github.com/xbitgo/core/log"
)

type poolConsumeTask struct {
	task ConsumeTask
	pool *ants.PoolWithFunc
	ch   chan []byte
}

func (p *poolConsumeTask) MQConsumer() MQ.Consumer {
	return p.task.MQConsumer()
}

func (p *poolConsumeTask) ConsumeJob(buf []byte) error {
	p.ch <- buf
	return nil
}

func (p *poolConsumeTask) TaskName() string {
	return p.task.TaskName()
}

func (p *poolConsumeTask) start() {
	for {
		select {
		case a := <-p.ch:
			go func(msg []byte) {
				err := p.pool.Invoke(msg)
				if err != nil {
					log.Errorf("asyncbus %s: %s, pool.Invoke err: %v", "poolConsumeTask", p.task.TaskName(), err)
				}
			}(a)
		}
	}
}

func WithPool(task ConsumeTask, size int) ConsumeTask {
	di.MustBind(task)
	pool, err := ants.NewPoolWithFunc(size, func(i interface{}) {
		err := task.ConsumeJob(i.([]byte))
		if err != nil {
			log.Errorf("asyncbus %s: %s, ConsumeJob err: %v, message: %s", "ConsumeJob[pool]", task.TaskName(), err, string(i.([]byte)))
		}
	})
	if err != nil {
		panic(err)
	}
	tk := &poolConsumeTask{
		task: task,
		pool: pool,
		ch:   make(chan []byte, 1),
	}
	go tk.start()
	return tk
}
