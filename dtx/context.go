package dtx

import (
	"context"
	"github.com/xbitgo/core/tools/tool_str"
	"google.golang.org/grpc/metadata"
	"time"
)

const (
	keyTxId   = "_dtx_tx_id"
	inSrvTxId = "_dtx_in_srv_tx_id"
)

type TxContext struct {
	_ctx       context.Context
	txId       string
	inSrvTxId  string
	withParent bool
}

func NewTxContext(ctx context.Context) *TxContext {
	withParent := true
	txId := onTx(ctx)
	if txId == "" {
		txId = tool_str.UUID()
		withParent = false
		ctx = metadata.NewOutgoingContext(ctx, metadata.New(map[string]string{
			keyTxId: txId,
		}))
	}
	tx := &TxContext{
		_ctx:       ctx,
		txId:       txId,
		withParent: withParent,
	}
	return tx
}

func NewInSrvTxContext(ctx context.Context) *TxContext {
	txId := onInSrvTx(ctx)
	if txId == "" {
		txId = tool_str.UUID()
	}
	tx := &TxContext{
		_ctx:      ctx,
		inSrvTxId: txId,
	}
	return tx
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

func onTx(ctx context.Context) (txId string) {
	switch _c := ctx.(type) {
	case *TxContext:
		return _c.txId
	default:
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return ""
		}
		if v, ok := md[keyTxId]; ok {
			if len(v) > 0 {
				txId = v[0]
			}
		}
	}
	return txId
}

func onInSrvTx(ctx context.Context) (txId string) {
	switch _c := ctx.(type) {
	case *TxContext:
		return _c.inSrvTxId
	default:
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return ""
		}
		if v, ok := md[inSrvTxId]; ok {
			if len(v) > 0 {
				txId = v[0]
			}
		}
	}
	return txId
}
