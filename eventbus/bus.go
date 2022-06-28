package eventbus

import (
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
	procNum      int
	events       chan *event
	addEvents    map[string]func(info Entity, args ...string) error
	modifyEvents map[string]func(new Entity, old Entity, args ...string) error
	deleteEvents map[string]func(info Entity, args ...string) error
	customEvents map[string]func(args ...string) error
}

func (b *Bus) run() {
	for i := 0; i < b.procNum; i++ {
		go func() {
			for e := range b.events {
				err := b.do(e)
				if err != nil {
					log.Errorf("event[%+v] do err:%v", *e, err)
				}
			}
		}()
	}
}

func (b *Bus) do(e *event) error {
	if e.Entity == nil {
		return nil
	}
	switch e.Action {
	case add:
		if fun, ok := b.addEvents[e.Entity.EntityName()]; ok {
			return fun(e.Entity, e.Args...)
		}
	case modify:
		if fun, ok := b.modifyEvents[e.Entity.EntityName()]; ok {
			return fun(e.Entity, e.OEntity, e.Args...)
		}
	case del:
		if fun, ok := b.deleteEvents[e.Entity.EntityName()]; ok {
			return fun(e.Entity, e.Args...)
		}
	}
	return errors.New("not found event subscriber")
}

func (b *Bus) Close() {
	//todo
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
