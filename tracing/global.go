package tracing

import (
	"context"
	"github.com/opentracing/opentracing-go"
	"github.com/uber/jaeger-client-go"
	"io"
)

type Tracer struct {
	tracer opentracing.Tracer
	closer io.Closer
}

var globalTracer *Tracer

// Close .
func Close() error {
	return globalTracer.closer.Close()
}

func (t *Tracer) Close() {
	_ = t.closer.Close()
}

// TraceID .
func TraceID(ctx context.Context) string {
	span := opentracing.SpanFromContext(ctx)
	spanCtx := span.Context()
	switch v := spanCtx.(type) {
	case jaeger.SpanContext:
		return v.TraceID().String()
	}
	return ""
}
