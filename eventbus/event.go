package eventbus

// EntityAdd 触发实体新增事件
func (e *event) EntityAdd(info Entity, args ...string) error {
	e.Action = add
	e.Args = args
	e.Entity = info
	if e.aSync {
		iBus.events <- e
		return nil
	}
	return iBus.do(e)
}

// EntityModify 触发实体变更事件
func (e *event) EntityModify(new Entity, old Entity, args ...string) error {
	e.Action = modify
	e.Args = args
	e.Entity = new
	e.OEntity = old
	if e.aSync {
		iBus.events <- e
		return nil
	}
	return iBus.do(e)
}

// EntityDelete 触发实体删除事件
func (e *event) EntityDelete(info Entity, args ...string) error {
	e.Action = del
	e.Args = args
	e.Entity = info
	if e.aSync {
		iBus.events <- e
		return nil
	}
	return iBus.do(e)
}

// Trigger 触发自定义事件
func (e *event) Trigger(funcName string, args ...string) error {
	e.Action = custom
	e.FuncName = funcName
	e.Args = args
	if e.aSync {
		iBus.events <- e
		return nil
	}
	return iBus.do(e)
}

// ASync 设置事件处理为异步执行
func (e *event) ASync() *event {
	e.aSync = true
	return e
}

type event struct {
	Entity   Entity
	OEntity  Entity
	Action   string
	Args     []string
	FuncName string
	aSync    bool
}
