package redeoraft_test

import (
	"time"

	"github.com/bsm/redeo"
	"github.com/bsm/redeoraft"
)

func ExampleNewTransport() {
	// Init server with default options
	srv := redeo.NewServer(nil)

	// Init a new transport, this installs three new commands on your
	// server:
	// * raftappend - appends replicated log entries from leader
	// * raftvote - replies to vote requests in an leadership election
	// * raftsnapshot - installs a snapshot
	tsp := redeoraft.NewTransport(srv, "10.0.0.1:9736", &redeoraft.Options{
		Timeout: time.Second,
	})
	defer tsp.Close()

	// Use the transport in your raft configuration
	// raftsrv, err := raft.NewRaft(..., tsp)
}
