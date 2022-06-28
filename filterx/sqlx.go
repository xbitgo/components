package filterx

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/xbitgo/core/tools/tool_convert"
	"strings"
)

func (f *Filtering) toSqlStr() (string, []interface{}, error) {
	field := f.Field
	args := make([]interface{}, 0)
	if f.Value == nil && f.Operator != OR {
		return "", nil, errors.New("Filtering Value is nil")
	}
	switch f.Operator {
	// 特殊操作
	case FieldEQ:
		return fmt.Sprintf(" %s = %s", field, f.Value), args, nil
	// 默认是EQUALS
	case "", EQ:
		args = append(args, f.Value)
		return fmt.Sprintf(" %s = ?", field), args, nil
	case NOT:
		args = append(args, f.Value)
		return fmt.Sprintf(" %s != ?", field), args, nil
	case IN:
		list, ok := f.sliceValue()
		if !ok {
			return "", args, errors.New("Filtering Value must be slice")
		}
		inStr := "'" + strings.Join(tool_convert.ToStringSlice(list), "','") + "'"
		return fmt.Sprintf(" %s IN (%s)", field, inStr), args, nil
	case NotIn:
		list, ok := f.sliceValue()
		if !ok {
			return "", args, errors.New("Filtering Value must be slice")
		}
		inStr := "'" + strings.Join(tool_convert.ToStringSlice(list), "','") + "'"
		return fmt.Sprintf(" %s NOT IN (%s)", field, inStr), args, nil
	case LessEQ:
		args = append(args, f.Value)
		return fmt.Sprintf(" %s <= ?", field), args, nil
	case Less:
		args = append(args, f.Value)
		return fmt.Sprintf(" %s < ?", field), args, nil
	case GreaterEQ:
		args = append(args, f.Value)
		return fmt.Sprintf(" %s >= ?", field), args, nil
	case Greater:
		args = append(args, f.Value)
		return fmt.Sprintf(" %s > ?", field), args, nil
	case Like:
		return fmt.Sprintf(" %s LIKE '%s'", field, "%"+tool_convert.ToString(f.Value)+"%"), args, nil
	case NotLike:
		return fmt.Sprintf(" %s NOT LIKE '%s'", field, "%"+tool_convert.ToString(f.Value)+"%"), args, nil
	case Between:
		list, ok := f.sliceValue()
		if !ok {
			return "", args, errors.New("Filtering Value must be slice")
		}
		if len(list) != 2 {
			return "", args, errors.New("[BETWEEN] Operator Value must be slice[2]{begin,end}")
		}
		args = append(args, list...)
		return fmt.Sprintf(" ( %s BETWEEN ? AND ? ) ", field), args, nil
	case FindInSet:
		list, ok := f.sliceValue()
		if !ok {
			args = append(args, f.Value)
			return fmt.Sprintf("FIND_IN_SET(?, %s)", field), args, nil
		}
		var OrSQL = ""
		for i := range list {
			if i == 0 {
				OrSQL = fmt.Sprintf("FIND_IN_SET(?, %s)", field)
				continue
			}
			OrSQL += fmt.Sprintf(" OR FIND_IN_SET(?, %s)", field)
		}
		args = append(args, list...)
		return OrSQL, args, nil
	case WHERE:
		args = append(args, f.Value)
		return f.Field, args, nil
	case OR:
		if f.Children == nil || len(f.Children) == 0 {
			return "", args, errors.New("operator[or] must be set children")
		}
		var (
			orSQL = ""
		)
		for i, f2 := range f.Children {
			_sqlStr, _args, err := f2.toSqlStr()
			args = append(args, _args...)
			if err != nil {
				return "", nil, err
			}
			if i == 0 {
				orSQL = _sqlStr
				continue
			}
			orSQL += " OR " + _sqlStr
		}
		return orSQL, args, nil
	default:
		return "", args, errors.New(fmt.Sprintf("operator[%s] invalid", f.Operator))
	}
}

func (list FilteringList) ToSqlWhere() (string, []interface{}, error) {
	var (
		sqlStr = ""
		args   = make([]interface{}, 0)
	)
	for i, v := range list {
		_str, _args, err := v.toSqlStr()
		if err != nil {
			return "", nil, err
		}
		args = append(args, _args...)
		if i == 0 {
			sqlStr += _str
			continue
		}
		switch v.Operator {
		case OR, FindInSet:
			_str = fmt.Sprintf(" (%s) ", _str)
		}
		sqlStr += " AND " + _str
	}
	return sqlStr, args, nil
}
