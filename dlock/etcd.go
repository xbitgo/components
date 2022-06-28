package dlock

import (
	"context"
	"fmt"
	"log"

	etcdv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type dEtcd struct {
	etcd         *etcdv3.Client
	lockName     string
	locked       bool
	session      *concurrency.Session
	eMux         *concurrency.Mutex
	beforeUnlock func()
	afterUnlock  func()
}

func NewEtcd(etcd *etcdv3.Client, lockName string) *dEtcd {
	return &dEtcd{
		etcd:     etcd,
		lockName: lockName,
	}
}

func (d *dEtcd) SetBeforeUnlock(fun func()) {
	d.beforeUnlock = fun
}

func (d *dEtcd) SetAfterUnlock(fun func()) {
	d.afterUnlock = fun
}

func (d *dEtcd) TryLock(ctx context.Context) error {
	session, err := concurrency.NewSession(d.etcd, concurrency.WithTTL(60))
	if err != nil {
		return err
	}
	d.session = session
	d.eMux = concurrency.NewMutex(session, fmt.Sprintf("dlock_%s", d.lockName))
	err = d.eMux.TryLock(ctx)
	if err != nil {
		return err
	}
	d.locked = true
	go func() {
		select {
		case <-d.session.Done():
			if err := d.UnLock(context.Background()); err != nil {
				log.Printf("dlock[etcd] session.Done unlock err: %v ", err)
			}
		}
	}()
	return nil
}

func (d *dEtcd) Lock(ctx context.Context) error {
	var (
		err     error
		session *concurrency.Session
	)
	// 创建会话
	for i := 0; i < 3; i++ {
		session, err = concurrency.NewSession(d.etcd, concurrency.WithTTL(60))
		if err == nil && session != nil {
			break
		}
		log.Printf("error dEtcd.Lock() concurrency.NewSession err: %v", err)
	}
	if err != nil {
		return err
	}
	fmt.Println(session)
	d.session = session
	d.eMux = concurrency.NewMutex(session, fmt.Sprintf("lock_%s", d.lockName))

	err = d.eMux.Lock(ctx)
	if err != nil {
		if err := d.session.Close(); err != nil {
			log.Printf("dlock[etcd] session.Close err: %v ", err)
		}
		return err
	}
	d.locked = true
	go func() {
		select {
		case <-d.session.Done():
			if err := d.UnLock(context.Background()); err != nil {
				log.Printf("dlock[etcd] session.Done unlock err: %v ", err)
			}
		}
	}()
	return nil
}

func (d *dEtcd) UnLock(ctx context.Context) error {
	if d.beforeUnlock != nil {
		d.beforeUnlock()
	}
	if d.afterUnlock != nil {
		defer d.afterUnlock()
	}
	if d.locked {
		err := d.eMux.Unlock(ctx)
		if err != nil {
			return err
		}
	}
	return d.session.Close()
}
