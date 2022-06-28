package crontab

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/robfig/cron/v3"
	"github.com/xbitgo/components/dlock"
	etcdv3 "go.etcd.io/etcd/client/v3"
)

type Crontab struct {
	locker  dlock.Locker
	leader  bool
	waiting bool
	rwMux   sync.RWMutex
	cron    *cron.Cron
}

func newCrontab(locker dlock.Locker) *Crontab {
	c := &Crontab{
		locker:  locker,
		leader:  false,
		waiting: false,
		rwMux:   sync.RWMutex{},
		cron:    cron.New(cron.WithSeconds()),
	}
	c.locker.SetBeforeUnlock(func() {
		c.rwMux.Lock()
		c.leader = false
		c.waiting = false
	})
	c.locker.SetAfterUnlock(func() {
		c.rwMux.Unlock()
	})
	_, err := c.cron.AddFunc("*/30 * * * * ?", c.cbLeader)
	if err != nil {
		log.Printf("error Crontab cbLeader err: %v", err)
	}
	return c
}

func NewByEtcd(etcd *etcdv3.Client, svcName string) *Crontab {
	return newCrontab(dlock.NewEtcd(etcd, fmt.Sprintf("crontab_%s", svcName)))
}

// SafeStart 增加recover保护的启动
func (c *Crontab) SafeStart() {
	// recover() 保护
	defer func() {
		if err := recover(); err != nil {
			log.Printf("panic: %v", err)
		}
	}()
	c.cbLeader()
	c.cron.Start()
}

// Start 一般启动
func (c *Crontab) Start() {
	c.cbLeader()
	c.cron.Start()
}

func (c *Crontab) isLeader() bool {
	c.rwMux.RLock()
	defer c.rwMux.RUnlock()
	return c.leader
}

func (c *Crontab) Close() {
	err := c.locker.UnLock(context.Background())
	if err != nil {
		log.Printf("error Crontab close err: %v", err)
	}
}

func (c *Crontab) cbLeader() {
	if c.waiting {
		return
	}
	c.waiting = true
	c.rwMux.Lock()
	defer c.rwMux.Unlock()
	err := c.locker.Lock(context.Background())
	if err != nil {
		log.Printf("Crontab fail lock err: %v", err)
		c.leader = false
		c.waiting = false
		return
	}
	c.leader = true
}

func (c *Crontab) AddFunc(spec string, fun func()) (cron.EntryID, error) {
	return c.cron.AddFunc(spec, func() {
		if c.isLeader() {
			fun()
		}
	})
}

func (c *Crontab) RemoveFunc(id cron.EntryID) {
	c.cron.Remove(id)
}
