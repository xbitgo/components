package dtx

import (
	"context"
	"encoding/json"
	"github.com/xbitgo/core/tools/tool_json"
	"time"
)

type Register interface {
	Rollback() error
	Commit() error
	Marshal() []byte
	Unmarshal(data []byte)
	Name() string
}

type SerialFunc struct {
	MD   map[string][]string `json:"md"`
	Func string              `json:"func"`
	Raw  json.RawMessage     `json:"args"`
}

func (f *SerialFunc) Marshal() []byte {
	buf, _ := tool_json.JSON.Marshal(f)
	return buf
}

func (f *SerialFunc) Unmarshal(data []byte) error {
	return tool_json.JSON.Unmarshal(data, f)
}

// register .
func (m *Manager) register(ctx context.Context, sf SerialFunc, actType string) error {
	// 判断是否在一个事务中
	if txId := onTx(ctx); txId != "" {
		return m.push(ctx, txId, actType, sf)
	}
	return nil
}

// registerInSrv .
func (m *Manager) registerInSrv(ctx context.Context, rollback func() error, commit func() error) {
	// 判断是否在一个事务中
	if txId := onInSrvTx(ctx); txId != "" {
		v, ok := m.inSrvTxs.Load(txId)
		if !ok {
			m.inSrvTxs.Store(txId, &inSrvTransaction{
				ID:        txId,
				timeoutAt: time.Now().Add(5 * time.Second),
				rollbacks: []func() error{rollback},
				commits:   []func() error{commit},
			})
			return
		}
		tx := v.(*inSrvTransaction)
		tx.rollbacks = append(tx.rollbacks, rollback)
		tx.commits = append(tx.commits, commit)
		m.inSrvTxs.Store(txId, tx)

	}
	return
}

func (m *Manager) TxKey(txId string, actType string) string {
	return "dtx:srv:" + m.serverName + ":" + txId + ":" + actType
}

func (m *Manager) push(ctx context.Context, txId string, actType string, sf SerialFunc) error {
	rs := m.redis.LPush(ctx, m.TxKey(txId, actType), sf.Marshal())
	if rs.Err() != nil {
		return rs.Err()
	}
	return nil
}

func (m *Manager) poll(ctx context.Context, txId string, actType string) ([]string, error) {
	rs := m.redis.RPopCount(ctx, m.TxKey(txId, actType), 100)
	if rs.Err() != nil {
		return nil, rs.Err()
	}
	return rs.Val(), nil
}
