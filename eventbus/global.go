package eventbus

var iBus *Bus

type Options struct {
	ProcNum int
}

var defaultOptions = Options{
	ProcNum: 16,
}

// Init procNum:异步处理协程数量
func Init(opts ...Options) {
	opt := defaultOptions
	if len(opts) > 0 {
		opt = opts[0]
	}
	iBus = &Bus{
		events:       make(chan *event, 1),
		addEvents:    map[string]func(info Entity, args ...string) error{},
		modifyEvents: map[string]func(new Entity, old Entity, args ...string) error{},
		deleteEvents: map[string]func(info Entity, args ...string) error{},
		customEvents: map[string]func(args ...string) error{},
		procNum:      opt.ProcNum,
	}
	iBus.run()
}

func Close() {
	iBus.Close()
}

func RegisterSubscriber(entityName string, subscriber Subscriber) {
	iBus.addEvents[entityName] = subscriber.Add
	iBus.modifyEvents[entityName] = subscriber.Modify
	iBus.deleteEvents[entityName] = subscriber.Delete
}

func RegisterCustomEvent(subscriber CustomSubscriber) {
	for s, f := range subscriber.RegisterFunc() {
		iBus.customEvents[s] = f
	}
}

// EntityAdd 触发实体新增事件
func EntityAdd(info Entity, args ...string) error {
	return iBus.EntityAdd(info, args...)
}

// EntityModify 触发实体变更事件
func EntityModify(new Entity, old Entity, args ...string) error {
	return iBus.EntityModify(new, old, args...)
}

// EntityDelete 触发实体删除事件
func EntityDelete(info Entity, args ...string) error {
	return iBus.EntityDelete(info, args...)
}

// Trigger 触发自定义事件
func Trigger(funcName string, args ...string) error {
	return iBus.Trigger(funcName, args...)
}

// ASync 设置事件处理为异步执行
func ASync() *event {
	return iBus.ASync()
}
