package traceing

import (
	"github.com/gin-gonic/gin"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

const (
	TraceIdKey = "X-Trace-ID"
	SpanIdKey  = "X-Span-ID"
)

func HTTPServerTrace() gin.HandlerFunc {
	return func(c *gin.Context) {
		var (
			tracer = opentracing.GlobalTracer()
			md     = metautils.ExtractIncoming(c)
			span   opentracing.Span
		)
		parentSpanCtx, err := tracer.Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(md))
		if err != nil || parentSpanCtx == nil {
			span = tracer.StartSpan("http: " + c.Request.URL.Path)
		} else {
			span = tracer.StartSpan("http: "+c.Request.URL.Path,
				opentracing.ChildOf(parentSpanCtx),
				ext.SpanKindRPCClient)
		}
		defer span.Finish()

		// todo

		c.Next()
	}
}
