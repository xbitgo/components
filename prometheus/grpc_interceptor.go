package prometheus

import (
	"context"
	"google.golang.org/grpc"
	"time"
)

// GrpcInterceptor .
func GrpcInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		st := time.Now()
		resp, err := handler(ctx, req)
		HistogramVec.Timing("grpc_"+MetricName(info.FullMethod), []string{"ret", RetLabel(err)}, st)
		return resp, err
	}
}
