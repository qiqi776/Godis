package grpcserver

import (
	"context"

	minikvv1 "mini-kv/api/minikv/v1"
	"mini-kv/internal/service/minikv"
)

type kvHandler struct {
	minikvv1.UnimplementedKVServer

	service minikv.Service
}

func newKVHandler(service minikv.Service) *kvHandler {
	return &kvHandler{service: service}
}

func (h *kvHandler) Get(ctx context.Context, req *minikvv1.GetRequest) (*minikvv1.GetResponse, error) {
	value, found, err := h.service.Get(ctx, req.GetKey())
	if err != nil {
		return nil, err
	}
	return &minikvv1.GetResponse{
		Value: value,
		Found: found,
	}, nil
}

func (h *kvHandler) Set(ctx context.Context, req *minikvv1.SetRequest) (*minikvv1.SetResponse, error) {
	if err := h.service.Set(ctx, req.GetKey(), req.GetValue()); err != nil {
		return nil, err
	}
	return &minikvv1.SetResponse{}, nil
}

func (h *kvHandler) Delete(ctx context.Context, req *minikvv1.DeleteRequest) (*minikvv1.DeleteResponse, error) {
	if err := h.service.Delete(ctx, req.GetKey()); err != nil {
		return nil, err
	}
	return &minikvv1.DeleteResponse{}, nil
}
