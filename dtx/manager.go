package dtx

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"github.com/xbitgo/core/di"
	"github.com/xbitgo/core/log"
	"github.com/xbitgo/core/tools/tool_json"
	"google.golang.org/grpc/metadata"
)

const (
	ActRollback = "rollback"
	ActCommit   = "commit"

	broadcast = "dtx:broadcast"
)

type inSrvTransaction struct {
	ID        string
	timeoutAt time.Time
	rollbacks []func() error
	commits   []func() error
}

type NotifyMsg struct {
	Act  string `json:"act"`
	TxId string `json:"txId"`
}

func (n *NotifyMsg) Marshal() []byte {
	buf, _ := tool_json.JSON.Marshal(n)
	return buf
}
func (n *NotifyMsg) Unmarshal(msg []byte) error {
	return tool_json.JSON.Unmarshal(msg, n)
}

var mg *Manager

type Manager struct {
	serverName string
	pool       *ants.PoolWithFunc
	notifyCh   chan []byte
	redis      redis.UniversalClient
	inSrvTxs   sync.Map
}

// Init  [redis version 6.2+]
func Init(serverName string, cli redis.UniversalClient, size int) {
	mg = &Manager{
		serverName: serverName,
		notifyCh:   make(chan []byte),
		redis:      cli,
		inSrvTxs:   sync.Map{},
	}
	pool, err := ants.NewPoolWithFunc(size, func(i interface{}) {
		err := mg.notifyDo(i.([]byte))
		if err != nil {
			log.Errorf("dtx notifyDo err: %v, message: %s", err, string(i.([]byte)))
		}
	})
	if err != nil {
		panic(err)
	}
	mg.pool = pool
	mg.run()
}

func (m *Manager) run() {
	// 定时清理
	tk := time.NewTicker(500 * time.Millisecond)
	go func() {
		for range tk.C {
			mg.inSrvTxs.Range(func(txId, value interface{}) bool {
				tx := value.(*inSrvTransaction)
				if tx.timeoutAt.Before(time.Now()) {
					mg.inSrvTxs.Delete(txId)
				}
				return true
			})
		}
	}()
	go func() {
		pubSub := m.redis.Subscribe(context.Background(), broadcast)
		for {
			select {
			case a := <-pubSub.Channel():
				msg := []byte(a.Payload)
				//fmt.Println("msg:", a.Payload)
				go func(msg []byte) {
					err := m.pool.Invoke(msg)
					if err != nil {
						log.Errorf("dtx pool.Invoke err: %v", err)
					}
				}(msg)
			}
		}
	}()
}

func (m *Manager) notifyDo(message []byte) error {
	n := &NotifyMsg{}
	err := n.Unmarshal(message)
	if err != nil {
		return err
	}
	switch n.Act {
	case ActRollback:
		m.rollback(n.TxId)
	case ActCommit:
		m.commit(n.TxId)
	}
	m.clean(n.TxId)
	return nil
}

func (m *Manager) clean(txId string) {
	_, _ = m.poll(context.Background(), txId, ActRollback)
	_, _ = m.poll(context.Background(), txId, ActCommit)
}

func (m *Manager) Tx(ctx context.Context, f func(ctx context.Context) error) error {
	_ctx := NewTxContext(ctx)
	if err := f(_ctx); err != nil {
		// 通知其他服务
		msg := NotifyMsg{
			Act:  ActRollback,
			TxId: _ctx.txId,
		}
		rs := m.redis.Publish(context.Background(), broadcast, msg.Marshal())
		if rs.Err() != nil {
			log.Errorf("dtx: txId: %s, rollback Publish err: %v", _ctx.txId, rs.Err())
		}
		return err
	}
	// 通知其他服务
	if !_ctx.withParent {
		msg := NotifyMsg{
			Act:  ActCommit,
			TxId: _ctx.txId,
		}
		rs := m.redis.Publish(context.Background(), broadcast, msg.Marshal())
		if rs.Err() != nil {
			log.Errorf("dtx: txId: %s, commit Publish err: %v", _ctx.txId, rs.Err())
		}
	}
	return nil
}

func (m *Manager) rollback(txId string) {
	list, err := m.poll(context.Background(), txId, ActRollback)
	if err != nil {
		log.Errorf("rollback poll txId: %s,err: %v", txId, err)
		return
	}
	// 从后向前回滚
	for i := len(list) - 1; i >= 0; i-- {
		rfStr := list[i]
		sf := SerialFunc{}
		err = sf.Unmarshal([]byte(rfStr))
		if err != nil {
			log.Errorf("dtx: txId: %s, rollback err: %v", txId, err)
		}
		err = m.serialCall(sf)
		if err != nil {
			log.Errorf("dtx: txId: %s, Rollback err: %v", txId, err)
		}
	}
	return
}

func (m *Manager) commit(txId string) {
	list, err := m.poll(context.Background(), txId, ActCommit)
	if err != nil {
		log.Errorf("rollback poll txId: %s,err: %v", txId, err)
		return
	}
	// 从前向后提交
	for _, rfStr := range list {
		sf := SerialFunc{}
		err = sf.Unmarshal([]byte(rfStr))
		if err != nil {
			log.Errorf("dtx: txId: %s, commit err: %v", txId, err)
		}
		err = m.serialCall(sf)
		if err != nil {
			log.Errorf("dtx: txId: %s, Commit err: %v", txId, err)
		}
	}
	return
}

func (m *Manager) serialCall(sf SerialFunc) error {
	if sf.Func == "" {
		return errors.New(fmt.Sprintf("illegal func[%s] ", sf.Func))
	}
	list := strings.Split(sf.Func, "@")
	if len(list) != 2 {
		return errors.New(fmt.Sprintf("illegal func[%s] ", sf.Func))
	}
	impl, fun := list[0], list[1]
	inst := di.GetInst(impl)
	if inst == nil {
		return errors.New(fmt.Sprintf("not found impl[%s] ", impl))
	}
	instv := reflect.ValueOf(inst)
	f := instv.MethodByName(fun)

	args := make([]reflect.Value, 0)
	curr := 0
	for i := 0; i < f.Type().NumIn(); i++ {
		arg := reflect.New(f.Type().In(i))
		switch f.Type().In(i).Kind() {
		case reflect.Interface:
			if f.Type().In(i).String() == "context.Context" {
				ctx := context.Background()
				ctx = metadata.NewIncomingContext(ctx, sf.MD)
				arg.Elem().Set(reflect.ValueOf(ctx))
				args = append(args, arg.Elem())
				continue
			}
			return errors.New(fmt.Sprintf("func[%s] param must be not interface type", sf.Func))
		default:
			arg.Elem().Set(reflect.ValueOf(sf.Raw))
			args = append(args, arg.Elem())
			curr++
		}
	}
	rs := f.Call(args)
	rsf := rs[0].Interface()
	if rsf == nil {
		return nil
	}
	return rsf.(error)
}

func (m *Manager) inSrvTx(ctx context.Context, f func(ctx context.Context) error) error {
	_ctx := NewInSrvTxContext(ctx)
	if err := f(_ctx); err != nil {
		m.inSrvRollback(_ctx.txId)
		return err
	}
	m.inSrvCommit(_ctx.txId)
	return nil
}

func (m *Manager) inSrvRollback(txId string) {
	v, ok := m.inSrvTxs.Load(txId)
	if !ok {
		return
	}
	tx := v.(*inSrvTransaction)
	for _, r := range tx.rollbacks {
		err := r()
		if err != nil {
			log.Errorf("dtx: txId: %s, rollback err: %v", txId, err)
		}
	}
	m.inSrvClose(txId)
	return
}

func (m *Manager) inSrvCommit(txId string) {
	v, ok := m.inSrvTxs.Load(txId)
	if !ok {
		return
	}
	tx := v.(*inSrvTransaction)
	for _, r := range tx.commits {
		err := r()
		if err != nil {
			log.Errorf("dtx: txId: %s, commit err: %v", txId, err)
		}
	}
	m.inSrvClose(txId)
	return
}

func (m *Manager) inSrvClose(txId string) {
	mg.inSrvTxs.Delete(txId)
}
