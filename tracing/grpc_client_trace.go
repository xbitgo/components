package tracing

import (
	"context"
	"encoding/base64"
	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/metadata"
	"strings"
	"time"
)

func GrpcClientTrace() grpc.UnaryClientInterceptor {
	return UnaryClientInterceptor(opentracing.GlobalTracer())
}

func UnaryClientInterceptor(tracer opentracing.Tracer) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		newCtx, span := newClientSpanFromContext(ctx, tracer, method)
		defer span.Finish()
		// 开始时间
		span.SetTag("startTime", time.Now())
		err := invoker(newCtx, method, req, reply, cc, opts...)
		// 结束时间
		span.SetTag("endTime", time.Now())
		// 请求方式
		span.SetTag("Method", method)
		// 请求
		span.LogKV("request", req)
		// 响应
		span.LogKV("result", reply)
		return err
	}
}

func newClientSpanFromContext(ctx context.Context, tracer opentracing.Tracer, fullMethodName string) (context.Context, opentracing.Span) {
	var parentSpanCtx opentracing.SpanContext
	if parent := opentracing.SpanFromContext(ctx); parent != nil {
		parentSpanCtx = parent.Context()
	}
	opts := []opentracing.StartSpanOption{
		opentracing.ChildOf(parentSpanCtx),
		ext.SpanKindRPCClient,
	}

	clientSpan := tracer.StartSpan("client:"+fullMethodName, opts...)
	md := metautils.ExtractOutgoing(ctx).Clone()
	if err := tracer.Inject(clientSpan.Context(), opentracing.HTTPHeaders, metadataTextMap(md)); err != nil {
		grpclog.Infof("grpc_client: failed serializing trace information: %v", err)
	}
	ctxWithMetadata := md.ToOutgoing(ctx)
	return opentracing.ContextWithSpan(ctxWithMetadata, clientSpan), clientSpan
}

// metadataTextMap extends a metadata.MD to be an opentracing textmap
type metadataTextMap metadata.MD

// Set is a opentracing.TextMapReader interface that extracts values.
func (m metadataTextMap) Set(key, val string) {
	// gRPC allows for complex binary values to be written.
	encodedKey, encodedVal := encodeKeyValue(key, val)
	// The metadata object is a multimap, and previous values may exist, but for opentracing headers, we do not append
	// we just override.
	m[encodedKey] = []string{encodedVal}
}

const (
	binHdrSuffix = "-bin"
)

// encodeKeyValue encodes key and value qualified for transmission via gRPC.
// note: copy pasted from private values of grpc.metadata
func encodeKeyValue(k, v string) (string, string) {
	k = strings.ToLower(k)
	if strings.HasSuffix(k, binHdrSuffix) {
		val := base64.StdEncoding.EncodeToString([]byte(v))
		v = val
	}
	return k, v
}
