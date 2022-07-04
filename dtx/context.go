package dtx

import (
	"context"
	"github.com/pkg/errors"
	"github.com/xbitgo/core/tools/tool_str"
	"time"
)

type TxContext struct {
	_ctx       context.Context
	txId       string
	parentTxID string
	timeoutAt  time.Time
}

func NewTxContext(ctx context.Context) (*TxContext, error) {
	_txID, parentTxID := mg.onTx(ctx)
	if parentTxID != "" {
		return nil, errors.New("不支持 两层以上分布式事务")
	}
	tx := &TxContext{
		_ctx:       ctx,
		txId:       tool_str.UUID(),
		parentTxID: _txID,
		timeoutAt:  time.Now().Add(1 * time.Minute),
	}
	return tx, nil
}

func (t *TxContext) Deadline() (deadline time.Time, ok bool) {
	return t._ctx.Deadline()
}

func (t *TxContext) Done() <-chan struct{} {
	return t._ctx.Done()
}

func (t *TxContext) Err() error {
	return t._ctx.Err()
}

func (t *TxContext) Value(key interface{}) interface{} {
	return t._ctx.Value(key)
}

func (t *TxContext) ParentContext() context.Context {
	return t._ctx
}
