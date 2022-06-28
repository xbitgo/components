package delaybus

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/xbitgo/core/log"
	"github.com/xbitgo/core/tools/tool_convert"
	"github.com/xbitgo/core/tools/tool_json"
	"github.com/xbitgo/core/tools/tool_str"
	"time"
)

const (
	delayKey = ":delaybus:delayer"
	zScript  = "local a=redis.call('ZRANGEBYSCORE',KEYS[1],ARGV[1],ARGV[2],ARGV[3],ARGV[4],ARGV[5]) for k,v in ipairs(a) do redis.call('ZREM',KEYS[1],v) end return a"
)

type RedisDelayer struct {
	cli       *redis.Client
	keyPrefix string
	dataChan  chan []byte
}

func NewRedisDelayer(cli *redis.Client, keyPrefix string) *RedisDelayer {
	return &RedisDelayer{
		cli:       cli,
		keyPrefix: keyPrefix,
		dataChan:  make(chan []byte, 1),
	}
}

func (r *RedisDelayer) delayKey() string {
	return r.keyPrefix + delayKey
}

func (r *RedisDelayer) Add(ctx context.Context, at time.Time, message []byte, key ...string) error {
	var _key string
	if len(key) > 0 {
		_key = key[0]
	} else {
		_key = tool_str.UUID()
	}
	it := delayMsg{
		Key: _key,
		Msg: message,
	}
	s, _ := tool_json.JSON.Marshal(it)
	z := &redis.Z{
		Score:  float64(at.Unix()),
		Member: string(s),
	}
	inc := r.cli.ZAdd(ctx, r.delayKey(), z)

	return inc.Err()
}

func (r *RedisDelayer) Consume() chan []byte {
	go func() {
		tk := time.NewTicker(100 * time.Millisecond)
		for {
			select {
			case <-tk.C:
				nms := tool_convert.Int64ToString(time.Now().Unix())
				for {
					list := make([]string, 0)
					cmd := r.cli.Eval(context.Background(), zScript, []string{r.delayKey()}, "0", nms, "LIMIT", 0, 100)
					ret, err := cmd.Result()
					if err != nil {
						break
					}
					buf, _ := tool_json.JSON.Marshal(ret)
					if string(buf) == "[]" {
						break
					}
					err = tool_json.JSON.Unmarshal(buf, &list)
					if err != nil {
						log.Errorf("[delaybus.RedisDelayer] item Unmarshal list err: %v", err)
						continue
					}
					for _, str := range list {
						item := delayMsg{}
						err = tool_json.JSON.Unmarshal([]byte(str), &item)
						if err != nil {
							log.Errorf("[delaybus.RedisDelayer] item Unmarshal err: %v", err)
							continue
						}
						r.dataChan <- item.Msg
					}
				}
			}
		}
	}()
	return r.dataChan
}
