# nrdgraph
[![GoDoc](https://godoc.org/github.com/izumin5210/nrdgraph?status.svg)](https://godoc.org/github.com/izumin5210/nrdgraph)
[![license](https://img.shields.io/github/license/izumin5210/nrdgraph.svg)](./LICENSE)

Dgraph client instrumentation for New Relic.

## Example

```go
addrs := []string{
	"dgraph-1.example.com",
	"dgraph-2.example.com",
	"dgraph-3.example.com",
}

clients := make([]api.DgraphClient, len(addrs), len(addrs))

for i, addr := range addrs {
	conn, _ := grpc.Dial(addr, grpc.WithInsecure())
	clients[i] = nrdgraph.Wrap(api.NewDgraphClient(d), txn, nrdgraph.WithHost(addr))
}

client := client.NewDgraphClient(clients...)
```
