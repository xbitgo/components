package tracing

import (
	"context"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/util/metautils"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

func GrpcServerTrace() grpc.UnaryServerInterceptor {
	return unaryServerInterceptor(opentracing.GlobalTracer())
}

func unaryServerInterceptor(tracer opentracing.Tracer) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		newCtx, span := newServerSpanFromInbound(ctx, tracer, info.FullMethod)
		defer span.Finish()
		// 开始时间
		span.SetTag("startTime", time.Now())
		resp, err := handler(newCtx, req)
		// 结束时间
		span.SetTag("endTime", time.Now())
		// 请求方式
		span.SetTag("Method", info.FullMethod)
		// 请求
		span.LogKV("request", req)
		// 响应
		span.LogKV("response", resp)
		return resp, err
	}
}

func newServerSpanFromInbound(ctx context.Context, tracer opentracing.Tracer, fullMethodName string) (context.Context, opentracing.Span) {
	md := metautils.ExtractIncoming(ctx)
	parentSpanContext, err := tracer.Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(md))
	if err != nil && err != opentracing.ErrSpanContextNotFound {
		grpclog.Infof("grpc_server: failed parsing trace information: %v", err)
	}
	serverSpan := tracer.StartSpan(
		"server:"+fullMethodName,
		ext.RPCServerOption(parentSpanContext),
	)

	return opentracing.ContextWithSpan(ctx, serverSpan), serverSpan
}
