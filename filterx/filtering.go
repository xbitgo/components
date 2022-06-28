package filterx

import (
	"reflect"
)

const (
	FieldEQ   = "FIELD_EQ" // 字段相等
	EQ        = "="
	NOT       = "!="
	IN        = "IN"
	NotIn     = "NOT IN"
	LessEQ    = "<="
	Less      = "<"
	GreaterEQ = ">="
	Greater   = ">"
	Like      = "LIKE"
	NotLike   = "NOT LIKE"
	Between   = "BETWEEN"
	FindInSet = "FIND_IN_SET"
	WHERE     = "WHERE"
	OR        = "OR"
)

type Filtering struct {
	Field    string        `json:"field,omitempty"`
	Operator string        `json:"operator,omitempty"`
	Value    interface{}   `json:"value,omitempty"`
	Children FilteringList `json:"children,omitempty"`
}

func (f *Filtering) sliceValue() ([]interface{}, bool) {
	var (
		list []interface{}
		ok   bool
	)
	if reflect.TypeOf(f.Value).Kind() == reflect.Slice {
		s := reflect.ValueOf(f.Value)
		for i := 0; i < s.Len(); i++ {
			ele := s.Index(i)
			list = append(list, ele.Interface())
		}
		ok = true
	}
	return list, ok
}

type FilteringList []*Filtering

type help struct {
	Name      string `json:"name"`
	ValueType string `json:"value_type"`
}

var OperatorHelp = map[string]help{
	FieldEQ: {
		Name:      "FieldEQ",
		ValueType: "string",
	},
	EQ: {
		Name:      "EQ",
		ValueType: "interface",
	},
	NOT: {
		Name:      "NOT",
		ValueType: "interface",
	},
	IN: {
		Name:      "IN",
		ValueType: "slice",
	},
	NotIn: {
		Name:      "NotIn",
		ValueType: "slice",
	},
	LessEQ: {
		Name:      "LessEQ",
		ValueType: "interface",
	},
	Less: {
		Name:      "Less",
		ValueType: "interface",
	},
	GreaterEQ: {
		Name:      "GreaterEQ",
		ValueType: "interface",
	},
	Greater: {
		Name:      "Greater",
		ValueType: "interface",
	},
	Like: {
		Name:      "Like",
		ValueType: "interface",
	},
	NotLike: {
		Name:      "NotLike",
		ValueType: "interface",
	},
	Between: {
		Name:      "Between",
		ValueType: "slice[2]",
	},
	FindInSet: {
		Name:      "FindInSet",
		ValueType: "slice or interface",
	},
	WHERE: {
		Name:      "WHERE",
		ValueType: "interface",
	},
	OR: {
		Name:      "WHERE",
		ValueType: "use children",
	},
}
