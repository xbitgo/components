package limit

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"testing"
	"time"
)

func TestTokenLimit(t *testing.T) {
	cli := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})
	const (
		total = 100
		rate  = 20
		burst = 20
	)
	l := NewTokenLimiter(rate, burst, cli, "tokenlimit")
	var allowed int
	for i := 0; i < total; i++ {
		time.Sleep(time.Second / time.Duration(total))
		if l.Allow(context.Background()) {
			allowed++
		}
		fmt.Println(i, allowed)
	}
}

func TestPeriodLimit(t *testing.T) {
	cli := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})

	const (
		seconds = 60
		total   = 100
		quota   = 5
	)
	l := NewPeriodLimit(seconds, quota, cli, "periodlimit", Align())
	var allowed, hitQuota, overQuota int
	ctx := context.Background()
	for i := 0; i < total; i++ {
		val, err := l.Take(ctx, "first")
		if err != nil {
			t.Error(err)
		}
		fmt.Println(val)
		switch val {
		case Allowed:
			allowed++
		case HitQuota:
			hitQuota++
		case OverQuota:
			overQuota++
		default:
			t.Error("unknown status")
		}
	}
}
