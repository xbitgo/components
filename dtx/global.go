package dtx

import (
	"context"
	"google.golang.org/grpc/metadata"
)

func RegisterInSrv(ctx context.Context, rollback func() error, commit func() error) {
	mg.registerInSrv(ctx, rollback, commit)
}

func RegisterRollback(ctx context.Context, fun string, paramsRaw []byte) error {
	md, _ := metadata.FromIncomingContext(ctx)
	rollback := SerialFunc{
		MD:   md,
		Func: fun,
		Raw:  paramsRaw,
	}
	return mg.register(ctx, rollback, ActRollback)
}

func RegisterCommit(ctx context.Context, fun string, paramsRaw []byte) error {
	md, _ := metadata.FromIncomingContext(ctx)
	commit := SerialFunc{
		MD:   md,
		Func: fun,
		Raw:  paramsRaw,
	}
	return mg.register(ctx, commit, ActCommit)
}

func Tx(ctx context.Context, f func(ctx context.Context) error) error {
	return mg.Tx(ctx, f)
}
