package nrdgraph

import (
	"context"

	"github.com/dgraph-io/dgo/protos/api"
	newrelic "github.com/newrelic/go-agent"
	"google.golang.org/grpc"
)

var (
	datastoreDgraph = newrelic.DatastoreProduct("Dgraph")
)

// Wrap creates wraps the DgraphClient to spport to creating newrelic segments on each requests.
func Wrap(dc api.DgraphClient, opts ...Option) api.DgraphClient {
	o := &options{}
	for _, f := range opts {
		f(o)
	}
	return &wrappedClient{
		dc:      dc,
		options: o,
	}
}

type wrappedClient struct {
	dc      api.DgraphClient
	options *options
}

func (c *wrappedClient) Query(ctx context.Context, in *api.Request, opts ...grpc.CallOption) (*api.Response, error) {
	if txn := newrelic.FromContext(ctx); txn != nil {
		params := make(map[string]interface{}, len(in.GetVars())+2)
		for k, v := range in.GetVars() {
			params[k] = v
		}
		params["start_ts"] = in.GetStartTs()
		params["lin_read"] = in.GetLinRead()
		seg := c.createSegment(txn)
		seg.Operation = "Query"
		seg.ParameterizedQuery = in.GetQuery()
		seg.QueryParameters = params
		defer seg.End()
	}
	return c.dc.Query(ctx, in, opts...)
}

func (c *wrappedClient) Mutate(ctx context.Context, in *api.Mutation, opts ...grpc.CallOption) (*api.Assigned, error) {
	if txn := newrelic.FromContext(ctx); txn != nil {
		seg := c.createSegment(txn)
		seg.Operation = "Mutate"
		seg.QueryParameters = map[string]interface{}{
			"set_json":              string(in.GetSetJson()),
			"delete_json":           string(in.GetDeleteJson()),
			"set_nquads":            string(in.GetSetNquads()),
			"del_nquads":            string(in.GetDelNquads()),
			"set":                   in.GetSet(),
			"del":                   in.GetDel(),
			"start_ts":              in.GetStartTs(),
			"commit_now":            in.GetCommitNow(),
			"ignore_index_conflict": in.GetIgnoreIndexConflict(),
		}
		defer seg.End()
	}
	return c.dc.Mutate(ctx, in, opts...)
}

func (c *wrappedClient) Alter(ctx context.Context, in *api.Operation, opts ...grpc.CallOption) (*api.Payload, error) {
	if txn := newrelic.FromContext(ctx); txn != nil {
		seg := c.createSegment(txn)
		seg.Operation = "Alter"
		seg.QueryParameters = map[string]interface{}{
			"drop_all":  in.GetDropAll(),
			"drop_attr": in.GetDropAttr(),
			"schema":    in.GetSchema(),
		}
		defer seg.End()
	}
	return c.dc.Alter(ctx, in, opts...)
}

func (c *wrappedClient) CommitOrAbort(ctx context.Context, in *api.TxnContext, opts ...grpc.CallOption) (*api.TxnContext, error) {
	if txn := newrelic.FromContext(ctx); txn != nil {
		seg := c.createSegment(txn)
		seg.Operation = "Commit"
		if in.Aborted {
			seg.Operation = "Abort"
		}
		seg.QueryParameters = map[string]interface{}{
			"start_ts":  in.GetStartTs(),
			"commit_ts": in.GetCommitTs(),
			"keys":      in.GetKeys(),
			"lin_read":  in.GetLinRead(),
		}
		defer seg.End()
	}
	return c.dc.CommitOrAbort(ctx, in, opts...)
}

func (c *wrappedClient) CheckVersion(ctx context.Context, in *api.Check, opts ...grpc.CallOption) (*api.Version, error) {
	if txn := newrelic.FromContext(ctx); txn != nil {
		seg := c.createSegment(txn)
		seg.Operation = "CheckVersion"
		defer seg.End()
	}
	return c.dc.CheckVersion(ctx, in, opts...)
}

func (c *wrappedClient) createSegment(txn newrelic.Transaction) newrelic.DatastoreSegment {
	return newrelic.DatastoreSegment{
		StartTime:    newrelic.StartSegmentNow(txn),
		Product:      datastoreDgraph,
		Host:         c.options.host,
		PortPathOrID: c.options.id,
		DatabaseName: c.options.databaseName,
	}
}
