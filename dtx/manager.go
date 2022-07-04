package dtx

import (
	"context"
	"fmt"
	"github.com/panjf2000/ants/v2"
	"github.com/pkg/errors"
	"github.com/xbitgo/components/MQ"
	"github.com/xbitgo/core/di"
	"github.com/xbitgo/core/log"
	"github.com/xbitgo/core/tools/tool_json"
	"google.golang.org/grpc/metadata"
	"reflect"
	"sync"
	"time"
)

const (
	ActRollback = 1
	ActClose    = 2

	keyTxId       = "_dtx_tx_id"
	keyParentTxId = "_dtx_parent_tx_id"
)

type transaction struct {
	ID              string
	timeoutAt       time.Time
	rollbacks       []func() error
	commits         []func() error
	serialRollbacks []SerialFunc
	serialCommits   []SerialFunc
}

type NotifyMsg struct {
	App        string `json:"app"`
	Act        int    `json:"act"`
	TxId       string `json:"txId"`
	ParentTxId string `json:"parentTxId"`
}

func (n NotifyMsg) Marshal() []byte {
	buf, _ := tool_json.JSON.Marshal(n)
	return buf
}
func (n NotifyMsg) Unmarshal(msg []byte) error {
	return tool_json.JSON.Unmarshal(msg, &n)
}

var mg *Manager

type Manager struct {
	txs      sync.Map
	pool     *ants.PoolWithFunc
	notifyCh chan []byte
	mq       MQ.Queue
}

func Init(mq MQ.Queue, size int) {
	mg = &Manager{
		txs:      sync.Map{},
		notifyCh: make(chan []byte),
		mq:       mq,
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
			mg.txs.Range(func(txId, value interface{}) bool {
				tx := value.(*transaction)
				if tx.timeoutAt.Before(time.Now()) {
					mg.txs.Delete(txId)
				}
				return true
			})
		}
	}()
	if m.mq == nil {
		return
	}
	go func() {
		err := m.mq.Consume(context.Background(), func(message []byte) error {
			m.notifyCh <- message
			return nil
		})
		if err != nil {
			log.Errorf("dtx mq.Consume err: %v", err)
		}
	}()
	go func() {
		for {
			select {
			case a := <-m.notifyCh:
				go func(msg []byte) {
					err := m.pool.Invoke(msg)
					if err != nil {
						log.Errorf("dtx pool.Invoke err: %v", err)
					}
				}(a)
			}
		}
	}()
}

func (m *Manager) notifyDo(message []byte) error {
	n := NotifyMsg{}
	err := n.Unmarshal(message)
	if err != nil {
		return err
	}
	switch n.Act {
	case ActRollback:
		m.rollback(n.TxId)
	case ActClose:
		if n.ParentTxId != "" {
			m.moveToParentTx(n.TxId, n.ParentTxId)
		} else {
			m.close(n.TxId)
		}
	}
	return nil
}

// register .
func (m *Manager) register(ctx context.Context, rollback func() error, commit func() error) {
	// 判断是否在一个事务中
	if txId, _ := m.onTx(ctx); txId != "" {
		v, ok := m.txs.Load(txId)
		if !ok {
			m.txs.Store(txId, &transaction{
				ID:        txId,
				timeoutAt: time.Now().Add(30 * time.Second),
				rollbacks: []func() error{rollback},
				commits:   []func() error{commit},
			})
			return
		}
		tx := v.(*transaction)
		tx.rollbacks = append(tx.rollbacks, rollback)
		tx.commits = append(tx.commits, commit)
		m.txs.Store(txId, tx)
	}
}

func (m *Manager) registerSerialFunc(ctx context.Context, rollback SerialFunc, commit SerialFunc) {
	// 判断是否在一个事务中
	if txId, _ := m.onTx(ctx); txId != "" {
		v, ok := m.txs.Load(txId)
		if !ok {
			m.txs.Store(txId, &transaction{
				ID:              txId,
				timeoutAt:       time.Now().Add(30 * time.Second),
				serialRollbacks: []SerialFunc{rollback},
				serialCommits:   []SerialFunc{commit},
			})
			return
		}
		tx := v.(*transaction)
		tx.serialRollbacks = append(tx.serialRollbacks, rollback)
		tx.serialCommits = append(tx.serialCommits, commit)
		m.txs.Store(txId, tx)
	}
}

func (m *Manager) onTx(ctx context.Context) (txId string, parentTxId string) {
	switch _c := ctx.(type) {
	case *TxContext:
		return _c.txId, _c.parentTxID
	default:
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return "", ""
		}
		if v, ok := md[keyTxId]; ok {
			if len(v) > 0 {
				txId = v[0]
			}
		}
		if v, ok := md[keyParentTxId]; ok {
			if len(v) > 0 {
				parentTxId = v[0]
			}
		}
	}
	return txId, parentTxId
}

func (m *Manager) InSrvTx(ctx context.Context, f func(ctx context.Context) error) error {
	_ctx, err := NewTxContext(ctx)
	if err != nil {
		return err
	}
	if err := f(_ctx); err != nil {
		m.rollback(_ctx.txId)
		return err
	}
	if _ctx.parentTxID == "" {
		m.close(_ctx.txId)
	} else {
		// txId 转移到父级
		m.moveToParentTx(_ctx.txId, _ctx.parentTxID)
	}
	return nil
}

func (m *Manager) MultiSrvTx(ctx context.Context, f func(ctx context.Context) error) error {
	_ctx, err := NewTxContext(ctx)
	__ctx := metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{
		keyTxId:       _ctx.txId,
		keyParentTxId: _ctx.parentTxID,
	}))
	_ctx = __ctx.(*TxContext)
	if err != nil {
		return err
	}
	if err := f(_ctx); err != nil {
		// 通知其他服务
		if m.mq != nil {
			msg := NotifyMsg{
				Act:        ActRollback,
				TxId:       _ctx.txId,
				ParentTxId: _ctx.parentTxID,
			}
			err := m.mq.Produce(context.Background(), msg.Marshal())
			if err != nil {
				log.Errorf("dtx: txId: %s, rollback Produce err: %v", _ctx.txId, err)
			}
		}
		return err
	}
	// 通知其他服务
	if m.mq != nil {
		msg := NotifyMsg{
			Act:        ActClose,
			TxId:       _ctx.txId,
			ParentTxId: _ctx.parentTxID,
		}
		err := m.mq.Produce(context.Background(), msg.Marshal())
		if err != nil {
			log.Errorf("dtx: txId: %s, rollback Produce err: %v", _ctx.txId, err)
		}
	}
	return nil
}

func (m *Manager) rollback(txId string) {
	v, ok := m.txs.Load(txId)
	if !ok {
		return
	}
	tx := v.(*transaction)
	for _, r := range tx.rollbacks {
		err := r()
		if err != nil {
			log.Errorf("dtx: txId: %s, rollback err: %v", txId, err)
		}
	}
	for _, r := range tx.serialRollbacks {
		err := m.SerialCall(r)
		if err != nil {
			log.Errorf("dtx: txId: %s, serial rollback err: %v", txId, err)
		}
	}
	return
}

func (m *Manager) close(txId string) {
	mg.txs.Delete(txId)
}

func (m *Manager) moveToParentTx(txId string, parentTxId string) {
	v, ok := m.txs.LoadAndDelete(txId)
	if !ok {
		return
	}
	m.txs.Store(parentTxId, v)
	return
}

func (m *Manager) SerialCall(sf SerialFunc) error {
	inst := di.GetInst(sf.Impl)
	if inst == nil {
		log.Errorf("")
		return errors.New(fmt.Sprintf("not found impl[%s] ", sf.Impl))
	}
	instv := reflect.ValueOf(inst)
	f := instv.MethodByName(sf.Func)
	if f.Type().NumIn() != len(sf.Args) {
		return errors.New(fmt.Sprintf("impl[%s] err params number", sf.Impl))
	}
	if f.Type().NumOut() != 1 {
		return errors.New(fmt.Sprintf("impl[%s] err return number", sf.Impl))
	}
	args := make([]reflect.Value, 0)
	for i := 0; i < f.Type().NumIn(); i++ {
		arg := reflect.New(f.Type().In(i))
		switch f.Type().In(i).Kind() {
		case reflect.Interface:
			return errors.New(fmt.Sprintf("impl[%s] param must be not interface type", sf.Impl))
		default:
			if !arg.CanSet() {
				arg.Elem().Set(reflect.ValueOf(sf.Args[i]))
			} else {
				arg.Set(reflect.ValueOf(sf.Args[i]))
			}
			args = append(args, arg.Elem())
		}
	}
	rs := f.Call(args)
	rsf := rs[0].Interface()
	if rsf == nil {
		return nil
	}
	return rsf.(error)
}
