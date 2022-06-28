package prometheus

import (
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// prometheus related param
var (
	Namespace              = "go"
	Subsystem              = "server"
	helpDescriptionMap     = make(map[string]string)
	histogramDefaultBucket = []float64{0.003, 0.005, 0.01, 0.03, 0.05, 0.07, 0.09, 0.1, 0.15, 0.2, 0.25, 0.3, 0.5, 0.7, 1, 1.5, 2, math.Inf(+1)}
	histogramBuckets       = make(map[string][]float64)
	constLabels            = make(map[string]string)
)

// AddConstLabels add const kv into promethues
func AddConstLabels(name string, value string) {
	constLabels[name] = value
}

// AddMetricDescription add metric desc, it's not thread safe, should initialize first
func AddMetricDescription(name string, desc string) {
	helpDescriptionMap[name] = desc
}

// getDescription result can not be empty
func getDescription(name string) string {
	if v := helpDescriptionMap[name]; v != "" {
		return v
	}

	return name
}

// AddHistogramBucket config histogramBuckets, it's not thread safe, should initialize first
// it will append +Inf as last element if not exist
func AddHistogramBucket(name string, buckets []float64) {
	if l := len(buckets); l != 0 && !math.IsInf(buckets[l-1], +1) {
		buckets = append(buckets, math.Inf(+1))
	}

	histogramBuckets[name] = buckets
}

type metricType string

const (
	cv metricType = "counterVec"
	gv metricType = "gaugeVec"
	hv metricType = "histogramVec"
	sv metricType = "summaryVec"
)

type metric struct {
	mt  metricType
	mu  *sync.RWMutex
	bag map[string]interface{}
}

func newMetric(mt metricType) *metric {
	return &metric{
		mt:  mt,
		mu:  &sync.RWMutex{},
		bag: make(map[string]interface{}),
	}
}

func (m *metric) gen(name string, labels []string) interface{} {
	switch m.mt {
	case cv:
		counterVec := prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace:   Namespace,
				Subsystem:   Subsystem,
				Name:        name,
				Help:        getDescription(name),
				ConstLabels: constLabels,
			},
			labels,
		)

		err := prometheus.Register(counterVec)
		if err != nil {
			return nil
		}

		return counterVec
	case gv:
		gaugeVec := prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   Namespace,
				Subsystem:   Subsystem,
				Name:        name,
				Help:        getDescription(name),
				ConstLabels: constLabels,
			}, labels,
		)

		err := prometheus.Register(gaugeVec)
		if err != nil {
			return nil
		}

		return gaugeVec
	case hv:
		opts := prometheus.HistogramOpts{
			Namespace:   Namespace,
			Subsystem:   Subsystem,
			Name:        name,
			Help:        getDescription(name),
			ConstLabels: constLabels,
			Buckets:     histogramDefaultBucket,
		}

		if v, ok := histogramBuckets[name]; ok {
			opts.Buckets = v
		}

		histogramVec := prometheus.NewHistogramVec(opts, labels)

		err := prometheus.Register(histogramVec)
		if err != nil {
			return nil
		}

		return histogramVec
	case sv:
		summaryVec := prometheus.NewSummaryVec(prometheus.SummaryOpts{
			Namespace:   Namespace,
			Subsystem:   Subsystem,
			Name:        name,
			Help:        getDescription(name),
			ConstLabels: constLabels,
		}, labels)

		err := prometheus.Register(summaryVec)
		if err != nil {
			return nil
		}

		return summaryVec
	default:
		return nil
	}
}

func (m *metric) get(name string, labels []string) interface{} {
	m.mu.RLock()

	if v, ok := m.bag[name]; ok {
		m.mu.RUnlock()
		return v
	}

	m.mu.RUnlock()

	// try again
	m.mu.Lock()
	if v, ok := m.bag[name]; ok {
		m.mu.Unlock()
		return v
	}

	v := m.gen(name, labels)
	m.bag[name] = v
	m.mu.Unlock()
	return v
}

type counter struct {
	m *metric
}

type gauge struct {
	m *metric
}

type histogram struct {
	m *metric
}

type summary struct {
	m *metric
}

func genLabels(kv interface{}) ([]string, []string) {
	var lbNames, lbValues []string
	switch v := kv.(type) {
	case []string:
		if l := len(v) % 2; l != 0 {
			v = v[:l-1]
		}
		for i, l := 0, len(v); i < l; i = i + 2 {
			lbNames = append(lbNames, v[i])
			lbValues = append(lbValues, v[i+1])
		}
	case map[string]string:
		for k := range v {
			lbNames = append(lbNames, k)
		}

		sort.Strings(lbNames)

		for i := range lbNames {
			lbValues = append(lbValues, v[lbNames[i]])
		}
	}
	return lbNames, lbValues
}

// CounterVec is promteheus counterVec
var CounterVec = &counter{m: newMetric(cv)}

// Count : kv can be pairs of []string or map[string]string
func (c *counter) Count(name string, kv interface{}, optionalNum ...float64) {
	lbNames, lbValues := genLabels(kv)

	v := c.m.get(name, lbNames).(*prometheus.CounterVec) // TODO it may nil?

	if len(optionalNum) == 0 || optionalNum[0] == 1 {
		v.WithLabelValues(lbValues...).Inc()
	} else {
		v.WithLabelValues(lbValues...).Add(optionalNum[0])
	}
}

// GaugeVec is promteheus gaugeVec
var GaugeVec = &gauge{m: newMetric(gv)}

// Set : kv can be pairs of []string or map[string]string
func (g *gauge) Set(name string, kv interface{}, num float64) {
	lbNames, lbValues := genLabels(kv)

	v := g.m.get(name, lbNames)
	if v != nil {
		vv, ok := v.(*prometheus.GaugeVec)
		if ok && vv != nil {
			vv.WithLabelValues(lbValues...).Set(num)
		}
	}
}

func (g *gauge) Add(name string, kv interface{}, num float64) {
	lbNames, lbValues := genLabels(kv)

	v := g.m.get(name, lbNames)
	if v != nil {
		vv, ok := v.(*prometheus.GaugeVec)
		if ok && vv != nil {
			vv.WithLabelValues(lbValues...).Add(num)
		}
	}
}

// HistogramVec is promteheus histogramVec
var HistogramVec = &histogram{m: newMetric(hv)}

// Timing : kv can be pairs of []string or map[string]string
func (h *histogram) Timing(name string, kv interface{}, startAt time.Time) {
	h.Observe(name, kv, time.Now().Sub(startAt).Seconds())
}

// Observe : kv can be pairs of []string or map[string]string
func (h *histogram) Observe(name string, kv interface{}, value float64) {
	lbNames, lbValues := genLabels(kv)

	v := h.m.get(name, lbNames)
	if v != nil {
		vv := v.(*prometheus.HistogramVec)
		vv.WithLabelValues(lbValues...).Observe(value)
	}

}

// SummaryVec is promteheus summaryVec
var SummaryVec = &summary{m: newMetric(sv)}

// Timing : kv can be pairs of []string or map[string]string
func (s *summary) Timing(name string, kv interface{}, startAt time.Time) {
	s.Observe(name, kv, time.Now().Sub(startAt).Seconds())
}

// Observe : kv can be pairs of []string or map[string]string
func (s *summary) Observe(name string, kv interface{}, value float64) {
	lbNames, lbValues := genLabels(kv)

	v := s.m.get(name, lbNames).(*prometheus.SummaryVec) // TODO it may nil?

	v.WithLabelValues(lbValues...).Observe(value)
}

const labErr = "0"
const labOK = "1"

// RetLabel RetLabel
func RetLabel(err error) string {
	if err == nil {
		return labOK
	}
	return labErr
}

// RetCount RetCount
func RetCount(c int) string {
	if c > 0 {
		return labOK
	}
	return labErr
}


func MetricName(s string) string {
	s = strings.TrimPrefix(s, "/")
	s = strings.Replace(s, "/", "_", -1)
	return strings.Replace(s, ".", "_", -1) + "_seconds"
}
