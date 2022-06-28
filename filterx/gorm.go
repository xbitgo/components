package filterx

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/xbitgo/core/tools/tool_convert"
	"gorm.io/gorm"
)

func (f *Filtering) GormOption(db *gorm.DB) (*gorm.DB, error) {
	field := f.Field
	if f.Value == nil && f.Operator != OR {
		return db, errors.New("Filtering Value is nil")
	}
	switch f.Operator {
	// 特殊操作
	case FieldEQ:
		return db.Where(fmt.Sprintf("%s = %s", field, f.Value)), nil
	// 默认是EQUALS
	case "", EQ:
		return db.Where(fmt.Sprintf("%s = ?", field), f.Value), nil
	case NOT:
		return db.Where(fmt.Sprintf("%s != ?", field), f.Value), nil
	case IN:
		return db.Where(fmt.Sprintf("%s IN ?", field), f.Value), nil
	case NotIn:
		return db.Where(fmt.Sprintf("%s NOT IN ?", field), f.Value), nil
	case LessEQ:
		return db.Where(fmt.Sprintf("%s <= ?", field), f.Value), nil
	case Less:
		return db.Where(fmt.Sprintf("%s <  ?", field), f.Value), nil
	case GreaterEQ:
		return db.Where(fmt.Sprintf("%s >= ?", field), f.Value), nil
	case Greater:
		return db.Where(fmt.Sprintf("%s > ?", field), f.Value), nil
	case Like:
		return db.Where(fmt.Sprintf("%s LIKE ?", field), "%"+tool_convert.ToString(f.Value)+"%"), nil
	case NotLike:
		return db.Where(fmt.Sprintf("%s NOT LIKE ?", field), "%"+tool_convert.ToString(f.Value)+"%"), nil
	case Between:
		list, ok := f.sliceValue()
		if !ok {
			return db, errors.New("Filtering Value must be slice")
		}
		if len(list) != 2 {
			return db, errors.New("[BETWEEN] Operator Value must be slice[2]{begin,end}")
		}
		begin, end := list[0], list[1]
		return db.Where(fmt.Sprintf("%s BETWEEN  ? AND ?", field), begin, end), nil
	case FindInSet:
		list, ok := f.sliceValue()
		if !ok {
			return db.Where(fmt.Sprintf("FIND_IN_SET(?, %s)", field), f.Value), nil
		}
		var orSQL = ""
		for i := range list {
			if i == 0 {
				orSQL = fmt.Sprintf("FIND_IN_SET(?, %s)", field)
				continue
			}
			orSQL += fmt.Sprintf(" OR FIND_IN_SET(?, %s)", field)
		}
		return db.Where(orSQL, list...), nil
	case WHERE:
		return db.Where(f.Field, f.Value), nil
	case OR:
		orSQL, args, err := f.toSqlStr()
		fmt.Println(err)
		if err != nil {
			return db, err
		}
		return db.Where(orSQL, args...), nil
	default:
		return db.Where("1 = 1"), nil
	}
}

func (list FilteringList) GormOption(db *gorm.DB) (*gorm.DB, error) {
	var err error
	for _, v := range list {
		db, err = v.GormOption(db)
		if err != nil {
			return db, err
		}
	}
	return db, nil
}
