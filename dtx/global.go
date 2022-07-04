package dtx

import "context"

type SerialFunc struct {
	Impl string
	Func string
	Args []interface{}
}

func Register(ctx context.Context, rollback func() error, commit func() error) {
	mg.register(ctx, rollback, commit)
}

func RegisterSerialFunc(ctx context.Context, rollback SerialFunc, commit SerialFunc) {
	mg.registerSerialFunc(ctx, rollback, commit)
}

func InSrvTx(ctx context.Context, f func(ctx context.Context) error) error {
	return mg.InSrvTx(ctx, f)
}

func MultiSrvTx(ctx context.Context, f func(ctx context.Context) error) error {
	return mg.MultiSrvTx(ctx, f)
}
