package asyncbus

import (
	"context"
	"github.com/robfig/cron/v3"
	"github.com/xbitgo/components/crontab"
	"github.com/xbitgo/core/di"
	"github.com/xbitgo/core/log"
	"time"
)

type Bus struct {
	tasks           []Task
	cancels         []func()
	cronCtl         *crontab.Crontab
	onceTasks       []onceBody
	onceTasksCronID cron.EntryID
}

type onceBody struct {
	task OnceTask
	at   time.Time
}

func NewBus(cronCtl *crontab.Crontab) *Bus {
	return &Bus{
		tasks:     make([]Task, 0),
		cancels:   make([]func(), 0),
		cronCtl:   cronCtl,
		onceTasks: make([]onceBody, 0),
	}
}

func (b *Bus) Register(task Task) {
	di.MustBind(task)
	switch task.(type) {
	case OnceTask, CronTask, ConsumeTask:
	default:
		panic("task illegal")
	}
	b.tasks = append(b.tasks, task)
}

func (b *Bus) Start() {
	for _, task := range b.tasks {
		switch tk := task.(type) {
		case OnceTask:
			b._onceTask(tk)
		case CronTask:
			b._cronTask(tk)
		case ConsumeTask:
			go b._consumeTask(tk)
		}
	}
	if len(b.onceTasks) > 0 {
		b._onceCronFunc()
	}
	go b.cronCtl.SafeStart()
}

func (b *Bus) _onceCronFunc() {
	cronId, err := b.cronCtl.AddFunc("@every 1s", func() {
		var tasks = make([]onceBody, 0)
		for _, body := range b.onceTasks {
			if body.at.Before(time.Now()) {
				err := body.task.Run()
				if err != nil {
					log.Errorf("asyncbus OnceTask: %s, run at: %s  err: %v", body.task.TaskName(), body.at.Format("2006-01-02 15:04:05"), err)
				} else {
					log.Infof("asyncbus OnceTask: %s, run at: %s  success", body.task.TaskName(), body.at.Format("2006-01-02 15:04:05"))
				}
				continue
			}
			tasks = append(tasks, body)
		}
		b.onceTasks = tasks
		if len(b.onceTasks) == 0 {
			b.cronCtl.RemoveFunc(b.onceTasksCronID)
		}
	})
	if err != nil {
		log.Errorf("asyncbus onceTasks.Cron err: %v", err)
	}
	b.onceTasksCronID = cronId
}

func (b *Bus) _onceTask(tk OnceTask) {
	b.onceTasks = append(b.onceTasks, onceBody{
		task: tk,
		at:   tk.At(),
	})
}

func (b *Bus) _cronTask(tk CronTask) {
	_, err := b.cronCtl.AddFunc(tk.CronSpec(), func() {
		err := tk.CronJob()
		if err != nil {
			log.Errorf("asyncbus CronTask: %s, CronJob err: %v", tk.TaskName(), err)
		}
	})
	if err != nil {
		log.Errorf("asyncbus CronTask: %s, cronCtl.Add err: %v", tk.TaskName(), err)
	}
}

func (b *Bus) _consumeTask(tk ConsumeTask) {
	ctx, cancel := context.WithCancel(context.Background())
	b.cancels = append(b.cancels, cancel)
	err := tk.MQConsumer().Consume(ctx, func(message []byte) error {
		err := tk.ConsumeJob(message)
		if err != nil {
			log.Errorf("asyncbus ConsumeTask: %s, ConsumeJob err: %v, message: %s", tk.TaskName(), err, string(message))
		}
		return err
	})
	if err != nil {
		log.Errorf("asyncbus ConsumeTask: %s, MQConsumer.Consume err: %v", tk.TaskName(), err)
	}
}

func (b *Bus) Close() {
	b.cronCtl.Close()
	for _, cancel := range b.cancels {
		cancel()
	}
}
