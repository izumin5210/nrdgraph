package nrdgraph

import (
	"context"

	"github.com/dgraph-io/dgraph/protos/api"
	"github.com/newrelic/go-agent"
	"google.golang.org/grpc"
)

var (
	datastoreDgraph = newrelic.DatastoreProduct("Dgraph")
)

// Wrap creates wraps the DgraphClient to spport to creating newrelic segments on each requests.
func Wrap(dc api.DgraphClient, txn newrelic.Transaction, opts ...Option) api.DgraphClient {
	o := &options{}
	for _, f := range opts {
		f(o)
	}
	return &wrappedClient{
		dc:      dc,
		txn:     txn,
		options: o,
	}
}

type wrappedClient struct {
	dc      api.DgraphClient
	txn     newrelic.Transaction
	options *options
}

func (c *wrappedClient) Query(ctx context.Context, in *api.Request, opts ...grpc.CallOption) (*api.Response, error) {
	if c.txn != nil {
		params := make(map[string]interface{}, len(in.GetVars())+2)
		for k, v := range in.GetVars() {
			params[k] = v
		}
		params["start_ts"] = in.GetStartTs()
		params["lin_read"] = in.GetLinRead()
		seg := c.createSegment(c.txn)
		defer seg.End()
		seg.Operation = "Query"
		seg.ParameterizedQuery = in.GetQuery()
		seg.QueryParameters = params
	}
	return c.dc.Query(ctx, in, opts...)
}

func (c *wrappedClient) Mutate(ctx context.Context, in *api.Mutation, opts ...grpc.CallOption) (*api.Assigned, error) {
	if c.txn != nil {
		seg := c.createSegment(c.txn)
		defer seg.End()
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
	}
	return c.dc.Mutate(ctx, in, opts...)
}

func (c *wrappedClient) Alter(ctx context.Context, in *api.Operation, opts ...grpc.CallOption) (*api.Payload, error) {
	if c.txn != nil {
		seg := c.createSegment(c.txn)
		defer seg.End()
		seg.Operation = "Alter"
		seg.QueryParameters = map[string]interface{}{
			"drop_all":  in.GetDropAll(),
			"drop_attr": in.GetDropAttr(),
			"schema":    in.GetSchema(),
		}
	}
	return c.dc.Alter(ctx, in, opts...)
}

func (c *wrappedClient) CommitOrAbort(ctx context.Context, in *api.TxnContext, opts ...grpc.CallOption) (*api.TxnContext, error) {
	if c.txn != nil {
		seg := c.createSegment(c.txn)
		defer seg.End()
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
	}
	return c.dc.CommitOrAbort(ctx, in, opts...)
}

func (c *wrappedClient) CheckVersion(ctx context.Context, in *api.Check, opts ...grpc.CallOption) (*api.Version, error) {
	if c.txn != nil {
		seg := c.createSegment(c.txn)
		defer seg.End()
		seg.Operation = "CheckVersion"
	}
	return c.dc.CheckVersion(ctx, in, opts...)
}

func (c *wrappedClient) createSegment(txn newrelic.Transaction) newrelic.DatastoreSegment {
	return newrelic.DatastoreSegment{
		StartTime:    newrelic.StartSegmentNow(c.txn),
		Product:      datastoreDgraph,
		Host:         c.options.host,
		PortPathOrID: c.options.id,
		DatabaseName: c.options.databaseName,
	}
}
