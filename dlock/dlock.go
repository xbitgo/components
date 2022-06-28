package dlock

import "context"

type Locker interface {
	TryLock(ctx context.Context) error // 尝试获取锁 失败立刻返回
	Lock(ctx context.Context) error    // 获取锁 阻塞直到获取锁
	UnLock(ctx context.Context) error
	SetBeforeUnlock(fun func())
	SetAfterUnlock(fun func())
}
