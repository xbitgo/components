package cfg_adapter

import (
	"fmt"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"

	"github.com/xbitgo/core/log"
)

type Zk struct {
	rootNode  string
	lock      sync.Mutex
	zkServers []string
	zkc       *zk.Conn
	values    map[string]string
	rs        interface{}
}

func NewZk(zkServers []string, rootNode string) (*Zk, error) {
	conn, event, err := zk.Connect(zkServers, 2*time.Second)
	if err != nil {
		return nil, err
	}
loop:
	for {
		e := <-event
		switch e.State {
		case zk.StateConnected:
			break loop
		case zk.StateDisconnected:
			return nil, errors.New("[cfg] disconnected from " + e.Server)
		}
	}
	return &Zk{
		rootNode:  rootNode,
		lock:      sync.Mutex{},
		zkServers: zkServers,
		zkc:       conn,
		values:    map[string]string{},
		rs:        nil,
	}, nil
}

func (z *Zk) Apply(rs interface{}, args ...string) error {
	if rs == nil {
		return errors.New("[cfg.zk] param rs is nil")
	}

	typ := reflect.TypeOf(rs)
	if typ.Kind() != reflect.Ptr {
		return errors.New("[cfg.zk] cannot apply to non-pointer struct")
	}
	if len(args) == 0 {
		return errors.New("[cfg.zk] illegal params args")
	}
	z.rs = rs
	return z.run(args...)
}

func (z *Zk) Close() error {
	z.zkc.Close()
	return nil
}

func (z *Zk) run(args ...string) error {
	z.lock.Lock()
	defer z.lock.Unlock()
	for _, arg := range args {
		zPath := fmt.Sprintf("%s/%s", z.rootNode, arg)
		children, _, err := z.zkc.Children(zPath)
		if err != nil {
			if err == zk.ErrNoNode {
				log.Errorf("[cfg.zk] node [%s] not exist,skip !", arg)
			}
			return err
		}
		if len(children) == 0 {
			log.Errorf("[cfg.zk] node [%s] child is null,skip !", arg)
			continue
		}
		for _, child := range children {
			leaf := fmt.Sprintf("%s/%s", zPath, child)
			data, _, err := z.zkc.Get(leaf)
			if err != nil {
				return err
			}
			z.values[child] = string(data)
			go z.watch(leaf, child)
		}
	}
	z.parse()
	return nil
}

func (z *Zk) watch(leaf string, child string) {
	_, _, event, err := z.zkc.GetW(leaf)
	if err != nil {
		log.Errorf("[cfg.zk] build watch err: %v", err)
		return
	}
	for {
		select {
		case e := <-event:
			if e.Type == zk.EventNodeDataChanged {
				data, _, eNew, err := z.zkc.GetW(leaf)
				if err != nil {
					log.Errorf("[cfg.zk] build watch err: %v", err)
					return
				}
				z.values[child] = string(data)
				z.lock.Lock()
				z.parse()
				z.lock.Unlock()
				event = eNew
				continue
			}
			log.Infof("[cfg.zk] zk event: %s", e.Type.String())
			_, _, eNew, err := z.zkc.GetW(leaf)
			if err != nil {
				log.Errorf("[cfg.zk] build watch err: %v", err)
				return
			}
			event = eNew
		}
	}
}

func (z *Zk) parse() {
	rs := z.rs
	var vMap = make(map[string]interface{})
	rt := reflect.TypeOf(rs)
	for i := 0; i < rt.Elem().NumField(); i++ {
		var (
			tag = rt.Elem().Field(i).Tag.Get("json")
			src = z.values[tag]
		)
		if tag == "" {
			continue
		}
		if src == "" {
			continue
		}
		kType := rt.Elem().Field(i).Type.Kind()
		if kType == reflect.Ptr {
			kType = rt.Elem().Field(i).Type.Elem().Kind()
		}
		switch kType {
		case reflect.Slice:
			tmp := make([]interface{}, 0)
			err := jsoniter.Unmarshal([]byte(src), &tmp)
			if err != nil {
				log.Errorf("[cfg] tag[%s] json UnmarshalFromString [%s] to Slice err %v", tag, src, err)
				continue
			}
			vMap[tag] = tmp
		case reflect.Struct:
			tmp := make(map[string]interface{}, 0)
			err := jsoniter.Unmarshal([]byte(src), &tmp)
			if err != nil {
				log.Errorf("[cfg] tag[%s] json UnmarshalFromString [%s] to Struct err %v", tag, src, err)
				continue
			}
			vMap[tag] = tmp
		case reflect.Map:
			tmp := make(map[string]interface{}, 0)
			err := jsoniter.Unmarshal([]byte(src), &tmp)
			if err != nil {
				log.Errorf("[cfg] tag[%s] json UnmarshalFromString [%s] to Map err %v", tag, src, err)
				continue
			}
			vMap[tag] = tmp
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			intVal, err := strconv.ParseInt(src, 10, 64)
			if err != nil {
				log.Errorf("[cfg] tag[%s] parse [%s] to Int err %v", tag, src, err)
				continue
			}
			vMap[tag] = intVal
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			intVal, err := strconv.ParseUint(src, 10, 64)
			if err != nil {
				log.Errorf("[cfg] tag[%s] parse [%s] to UInt err %v", tag, src, err)
				continue
			}
			vMap[tag] = intVal
		case reflect.Float32, reflect.Float64:
			floatVal, err := strconv.ParseFloat(src, 64)
			if err != nil {
				log.Errorf("[cfg] tag[%s] parse [%s] to Float err %v", tag, src, err)
				continue
			}
			vMap[tag] = floatVal
		case reflect.Bool:
			boolVal, err := strconv.ParseBool(src)
			if err != nil {
				log.Errorf("[cfg] tag[%s] parse [%s] to Float err %v", tag, src, err)
				continue
			}
			vMap[tag] = boolVal
		default:
			vMap[tag] = src
		}
	}

	rBuf, _ := jsoniter.Marshal(vMap)
	err := jsoniter.Unmarshal(rBuf, &rs)
	if err != nil {
		log.Errorf("[cfg] parse to rs err: %v", err)
		return
	}
}
