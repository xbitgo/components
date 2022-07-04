package sequence

import (
	"github.com/pkg/errors"
	"os"
	"strconv"
	"time"

	"github.com/sony/sonyflake"
)

var (
	sequence *sonyflake.Sonyflake
)

func defaultInit() {
	setting := sonyflake.Settings{}
	// 使用env 配置
	envNode := os.Getenv("SEQUENCE_NODE")
	if envNode != "" {
		node, err := strconv.ParseInt(envNode, 10, 64)
		if err == nil {
			setting.MachineID = func() (uint16, error) {
				return uint16(node), nil
			}
		}
	}
	sequence = sonyflake.NewSonyflake(setting)
}

func Init() {
	defaultInit()
}

func InitWithNode(node uint16, st ...time.Time) {
	setting := sonyflake.Settings{}
	setting.MachineID = func() (uint16, error) {
		return node, nil
	}
	if len(st) > 0 {
		setting.StartTime = st[0]
	}
	sequence = sonyflake.NewSonyflake(setting)
	return
}

// ID id
func ID() (int64, error) {
	if sequence == nil {
		return 0, errors.New("sequence not init")
	}
	sid, err := sequence.NextID()
	return int64(sid), err
}
