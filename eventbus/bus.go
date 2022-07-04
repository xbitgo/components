package eventbus

import (
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"github.com/xbitgo/core/log"
)

const (
	add    = "add"
	modify = "modify"
	del    = "delete"
	custom = "custom"
)

type Bus struct {
	maxProc      int
	procPool     *ants.PoolWithFunc
	events       chan *event
	addEvents    map[string]func(info Entity, args ...string) error
	modifyEvents map[string]func(new Entity, old Entity, args ...string) error
	deleteEvents map[string]func(info Entity, args ...string) error
	customEvents map[string]func(args ...string) error
}

func (b *Bus) run() {
	pool, err := ants.NewPoolWithFunc(b.maxProc, func(i interface{}) {
		e := i.(*event)
		err := b.do(i.(*event))
		if err != nil {
			log.Errorf("eventbus event[%+v] do err:%v", *e, err)
		}
	})
	if err != nil {
		log.Panicf("eventbus start ProcPool err:%v", err)
		panic(err)
	}
	b.procPool = pool

	go func() {
		for {
			select {
			case a := <-b.events:
				go func(e *event) {
					err := b.procPool.Invoke(e)
					if err != nil {
						log.Errorf("eventbus procPool.Invoke err: %v", err)
					}
				}(a)
			}
		}
	}()
}

func (b *Bus) do(e *event) error {
	switch e.Action {
	case add:
		if e.Entity == nil {
			return nil
		}
		if fun, ok := b.addEvents[e.Entity.EntityName()]; ok {
			return fun(e.Entity, e.Args...)
		}
	case modify:
		if e.Entity == nil {
			return nil
		}
		if fun, ok := b.modifyEvents[e.Entity.EntityName()]; ok {
			return fun(e.Entity, e.OEntity, e.Args...)
		}
	case del:
		if e.Entity == nil {
			return nil
		}
		if fun, ok := b.deleteEvents[e.Entity.EntityName()]; ok {
			return fun(e.Entity, e.Args...)
		}
	case custom:
		if fun, ok := b.customEvents[e.FuncName]; ok {
			return fun(e.Args...)
		}
	}
	return errors.New("not found event subscriber")
}

func (b *Bus) Close() {
	b.procPool.Release()
}

// EntityAdd 触发实体新增事件
func (b *Bus) EntityAdd(info Entity, args ...string) error {
	return new(event).EntityAdd(info, args...)
}

// EntityModify 触发实体变更事件
func (b *Bus) EntityModify(newInfo Entity, oldInfo Entity, args ...string) error {
	return new(event).EntityModify(newInfo, oldInfo, args...)
}

// EntityDelete 触发实体删除事件
func (b *Bus) EntityDelete(info Entity, args ...string) error {
	return new(event).EntityDelete(info, args...)
}

// Trigger 触发自定义事件
func (b *Bus) Trigger(funcName string, args ...string) error {
	return new(event).Trigger(funcName, args...)
}

// ASync 设置事件处理为异步执行
func (b *Bus) ASync() *event {
	return new(event).ASync()
}

type Entity interface {
	EntityName() string
}

type Subscriber interface {
	Add(info Entity, args ...string) error
	Modify(newInfo Entity, oldInfo Entity, args ...string) error
	Delete(info Entity, args ...string) error
}

type CustomSubscriber interface {
	RegisterFunc() map[string]func(args ...string) error
}
