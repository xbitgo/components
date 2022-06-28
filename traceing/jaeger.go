package traceing

import (
	"fmt"
	"github.com/opentracing/opentracing-go"
	jaegerCfg "github.com/uber/jaeger-client-go/config"
	"github.com/uber/jaeger-client-go/log"
	"github.com/uber/jaeger-lib/metrics"
	"io"
)

// InitJaegerTracer ...
func InitJaegerTracer(c *jaegerCfg.Configuration) {
	tracer, closer, err := newJaegerTracer(c)
	if err != nil {
		panic(fmt.Sprintf("InitTracer failed: %v\n", err))
	}
	globalTracer = &Tracer{
		tracer: tracer,
		closer: closer,
	}
	opentracing.SetGlobalTracer(tracer)
}

// newTrace¬
func newJaegerTracer(jc *jaegerCfg.Configuration) (opentracing.Tracer, io.Closer, error) {
	return jc.NewTracer(jaegerCfg.Logger(log.StdLogger), jaegerCfg.Metrics(metrics.NullFactory))
}
