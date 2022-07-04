package dtx

import (
	"fmt"
	"gorm.io/gorm"
	"reflect"

	"github.com/pkg/errors"
)

const (
	SET = SetOp(0)
	// ADD 字段加
	ADD = SetOp(1)
	// SUB 字段减
	SUB = SetOp(2)
	// REPLACE 字符串替换
	REPLACE = SetOp(3)
)

type SetOp int

type SetItem struct {
	Field    string      `json:"field,omitempty"`
	Operator SetOp       `json:"operator,omitempty"`
	Value    interface{} `json:"value,omitempty"`
}

type SetItemList []*SetItem

func (i SetItemList) ToGormUpdates() (map[string]interface{}, error) {
	updates := make(map[string]interface{})
	for _, incr := range i {
		err := incr.ToGormUpdates(updates)
		if err != nil {
			return updates, err
		}
	}
	return updates, nil
}

func (i SetItemList) RollbackGormUpdates(sets map[string]interface{}) (map[string]interface{}, error) {
	for _, incr := range i {
		err := incr.RollbackGormUpdates(sets)
		if err != nil {
			return sets, err
		}
	}
	return sets, nil
}

func (i *SetItem) ToGormUpdates(updates map[string]interface{}) error {
	switch i.Operator {
	case SET:
		updates[i.Field] = i.Value
	case ADD:
		updates[i.Field] = gorm.Expr(fmt.Sprintf("%s + ?", i.Field), i.Value)
	case SUB:
		updates[i.Field] = gorm.Expr(fmt.Sprintf("%s - ?", i.Field), i.Value)
	case REPLACE:
		list, ok := i.sliceValue()
		if !ok || len(list) != 2 {
			return errors.New("[REPLACE] Operator Value must be slice[2]{old,new}")
		}
		updates[i.Field] = gorm.Expr(fmt.Sprintf("replace(%s,?,?)", i.Field), list[0], list[1])
	}
	return nil
}

func (i *SetItem) RollbackGormUpdates(updates map[string]interface{}) error {
	switch i.Operator {
	case SET:
		return nil
		//updates[i.Field] = gorm.Expr(fmt.Sprintf("%s - ?", i.Field), i.Value)
	case ADD:
		updates[i.Field] = gorm.Expr(fmt.Sprintf("%s - ?", i.Field), i.Value)
	case SUB:
		updates[i.Field] = gorm.Expr(fmt.Sprintf("%s + ?", i.Field), i.Value)
	case REPLACE:
		list, ok := i.sliceValue()
		if !ok || len(list) != 2 {
			return errors.New("[REPLACE] Operator Value must be slice[2]{old,new}")
		}
		updates[i.Field] = gorm.Expr(fmt.Sprintf("replace(%s,?,?)", i.Field), list[1], list[0])
	}
	return nil
}

func (i *SetItem) sliceValue() ([]interface{}, bool) {
	var (
		list []interface{}
		ok   bool
	)
	if reflect.TypeOf(i.Value).Kind() == reflect.Slice {
		s := reflect.ValueOf(i.Value)
		for i := 0; i < s.Len(); i++ {
			ele := s.Index(i)
			list = append(list, ele.Interface())
		}
		ok = true
	}
	return list, ok
}
