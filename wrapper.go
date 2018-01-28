package nrdgraph

import (
	"context"

	"github.com/dgraph-io/dgraph/protos/api"
	"google.golang.org/grpc"
)

// Wrap creates wraps the DgraphClient to spport to creating newrelic segments on each requests.
func Wrap(dc api.DgraphClient) api.DgraphClient {
	return &wrappedClient{
		dc: dc,
	}
}

type wrappedClient struct {
	dc api.DgraphClient
}

func (c *wrappedClient) Query(ctx context.Context, in *api.Request, opts ...grpc.CallOption) (*api.Response, error) {
	return d.dc.Query(ctx, in, opts...)
}

func (c *wrappedClient) Mutate(ctx context.Context, in *api.Mutation, opts ...grpc.CallOption) (*api.Assigned, error) {
	return d.dc.Mutate(ctx, in, opts...)
}

func (c *wrappedClient) Alter(ctx context.Context, in *api.Operation, opts ...grpc.CallOption) (*api.Payload, error) {
	return d.dc.Alter(ctx, in, opts...)
}

func (c *wrappedClient) CommitOrAbort(ctx context.Context, in *api.TxnContext, opts ...grpc.CallOption) (*api.TxnContext, error) {
	return d.dc.CommitOrAbort(ctx, in, opts...)
}

func (c *wrappedClient) CheckVersion(ctx context.Context, in *api.Check, opts ...grpc.CallOption) (*api.Version, error) {
	return d.dc.CheckVersion(ctx, in, opts...)
}
