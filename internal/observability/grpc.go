package observability

import (
	"context"
	"path"
	"strings"
	"time"

	"google.golang.org/grpc"
)

func UnaryServerInterceptor(registry *Registry) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		startedAt := time.Now()
		resp, err := handler(ctx, req)
		if registry != nil {
			registry.ObserveGRPC(grpcMethodName(info.FullMethod), time.Since(startedAt), err)
		}
		return resp, err
	}
}

func grpcMethodName(fullMethod string) string {
	base := path.Base(fullMethod)
	if base == "." || base == "/" || base == "" {
		return strings.TrimPrefix(fullMethod, "/")
	}
	return base
}
