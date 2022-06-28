package sequence

import (
	"os"
	"strconv"

	"github.com/bwmarrin/snowflake"

	"github.com/xbitgo/core/log"
	"github.com/xbitgo/core/tools/tool_ip"
)

var (
	sequence *snowflake.Node
)

func defaultInit() error {
	var (
		node int64
		err  error
	)
	// 使用env 配置
	envNode := os.Getenv("SEQUENCE_NODE")
	if envNode != "" {
		node, err = strconv.ParseInt(envNode, 10, 64)
		if err != nil {
			return err
		}
	} else {
		ipu, err := tool_ip.Lower16BitPrivateIP()
		if err != nil {
			log.Errorf("Lower16BitPrivateIP err:%v", err)
			return err
		}
		node = int64(ipu)
	}
	sn, err := snowflake.NewNode(node)
	if err != nil {
		log.Errorf("NewNode err:%v", err)
		return err
	}
	sequence = sn
	return nil
}

func Init() error {
	return defaultInit()
}

func InitWithNode(node int64) error {
	sn, err := snowflake.NewNode(node)
	if err != nil {
		log.Errorf("NewNode err:%v", err)
		return err
	}
	sequence = sn
	return nil
}

// ID id
func ID() int64 {
	if sequence == nil {
		panic("sequence not init")
	}
	sid := sequence.Generate()
	return sid.Int64()
}
