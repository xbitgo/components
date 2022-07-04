package filterx

import (
	"fmt"
	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
	"github.com/xbitgo/core/tools/tool_convert"
)

func (list FilteringList) ElasticOption() (*elastic.BoolQuery, error) {
	var err error
	query := elastic.NewBoolQuery()
	for _, v := range list {
		query, err = v.elasticOp(query)
		if err != nil {
			return query, err
		}
	}
	return query, nil
}

func (f Filtering) elasticOp(query *elastic.BoolQuery) (*elastic.BoolQuery, error) {
	field := f.Field
	if f.Value == nil && f.Operator != OR {
		return query, errors.New("Filtering Value is nil")
	}

	switch f.Operator {
	// 特殊操作
	case FieldEQ:
		script := fmt.Sprintf("doc['%s.value'] == doc['%s.value']", field, tool_convert.ToString(f.Value))
		return query.Filter(elastic.NewScriptQuery(elastic.NewScript(script))), nil
	// 默认是EQUALS
	case "", EQ:
		return query.Filter(elastic.NewTermQuery(field, f.Value)), nil
	case NOT:
		return query.MustNot(elastic.NewTermQuery(field, f.Value)), nil
	case IN:
		return query.Filter(elastic.NewTermsQuery(field, f.Value)), nil
	case NotIn:
		return query.MustNot(elastic.NewTermsQuery(field, f.Value)), nil
	case LessEQ:
		return query.Filter(elastic.NewRangeQuery(field).Lte(f.Value)), nil
	case Less:
		return query.Filter(elastic.NewRangeQuery(field).Lt(f.Value)), nil
	case GreaterEQ:
		return query.Filter(elastic.NewRangeQuery(field).Gte(f.Value)), nil
	case Greater:
		return query.Filter(elastic.NewRangeQuery(field).Gt(f.Value)), nil
	case Like:
		return query.Filter(elastic.NewMatchPhraseQuery(field, f.Value)), nil
	case NotLike:
		return query.MustNot(elastic.NewMatchPhraseQuery(field, f.Value)), nil
	case Between:
		list, ok := f.sliceValue()
		if !ok {
			return query, errors.New("Filtering Value must be slice")
		}
		if len(list) != 2 {
			return query, errors.New("[BETWEEN] Operator Value must be slice[2]{begin,end}")
		}
		begin, end := list[0], list[1]
		return query.Filter(elastic.NewRangeQuery(field).Gte(begin).Lte(end)), nil
	case FindInSet:
		list, ok := f.sliceValue()
		if !ok {
			return query.Filter(elastic.NewMatchPhraseQuery(field, f.Value)), nil
		}
		children := make([]elastic.Query, 0)
		for _, v := range list {
			children = append(children, elastic.NewMatchPhraseQuery(field, v))
		}
		return query.Should(children...), nil
	case WHERE:
		return query, errors.New("elastic: Operator [WHERE] not supported")
	case OR:
		children, err := f.Children.ElasticOption()
		if err != nil {
			return query, err
		}
		return query.Should(children), nil
	default:
		return query, nil
	}

}
