package cfg_adapter

import (
	"reflect"
	"strconv"
	"sync"

	"github.com/apolloconfig/agollo/v4"
	agolloCfg "github.com/apolloconfig/agollo/v4/env/config"
	"github.com/apolloconfig/agollo/v4/storage"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"

	"github.com/xbitgo/core/log"
	"github.com/xbitgo/core/tools/tool_convert"
)

type Apollo struct {
	mux       sync.Mutex
	client    agollo.Client
	namespace string
	values    map[string]string
	rs        interface{}
}

// NewApollo .
func NewApollo(apolloCfg *agolloCfg.AppConfig, namespace string) (*Apollo, error) {
	cli, err := agollo.StartWithConfig(func() (*agolloCfg.AppConfig, error) {
		return &agolloCfg.AppConfig{
			AppID:         apolloCfg.AppID,
			Cluster:       apolloCfg.Cluster,
			IP:            apolloCfg.IP,
			NamespaceName: namespace,
			Secret:        apolloCfg.Secret,
		}, nil
	})
	if err != nil {
		return nil, err
	}
	c := &Apollo{
		mux:       sync.Mutex{},
		client:    cli,
		namespace: namespace,
		values:    map[string]string{},
		rs:        nil,
	}
	return c, nil
}

func (a *Apollo) Apply(rs interface{}) error {
	if rs == nil {
		return errors.New("[cfg.apollo] param rs is nil")
	}
	typ := reflect.TypeOf(rs)
	if typ.Kind() != reflect.Ptr {
		return errors.New("[cfg.apollo] cannot apply to non-pointer struct")
	}
	a.rs = rs
	err := a.get()
	if err != nil {
		return err
	}
	a.parse()
	a.client.AddChangeListener(a)
	return nil
}

func (a *Apollo) get() error {
	a.mux.Lock()
	defer a.mux.Unlock()
	store := a.client.GetConfigCache(a.namespace)
	if store == nil {
		log.Errorf("[cfg.apollo] namespace [%s] not exist,skip !", a.namespace)
	}
	store.Range(func(key, value interface{}) bool {
		k := tool_convert.ToString(key)
		v := tool_convert.ToString(value)
		if key == "" {
			return true
		}
		a.values[k] = v
		return true
	})
	return nil
}

func (a *Apollo) OnChange(event *storage.ChangeEvent) {
	//fmt.Printf("OnChange: %+v \n", event)
}

//OnNewestChange 监控最新变更
func (a *Apollo) OnNewestChange(event *storage.FullChangeEvent) {
	a.mux.Lock()
	defer a.mux.Unlock()
	for k, value := range event.Changes {
		v := tool_convert.ToString(value)
		a.values[k] = v
	}
	a.parse()
	log.Infof("[cfg] %s OnNewestChange parsed", a.namespace)
}

func (a *Apollo) parse() {
	rs := a.rs
	var vMap = make(map[string]interface{})
	rt := reflect.TypeOf(rs)
	for i := 0; i < rt.Elem().NumField(); i++ {
		var (
			tag = rt.Elem().Field(i).Tag.Get("json")
			src = a.values[tag]
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
