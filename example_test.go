package redeoraft_test

import (
	"io"
	"time"

	"github.com/bsm/redeo"
	"github.com/bsm/redeoraft"
	"github.com/hashicorp/raft"
)

func ExampleTransport() {
	// Init server with default config
	srv := redeo.NewServer(nil)

	// Init a new transport, this installs three new commands on your
	// server:
	// * raftappend - appends replicated log entries from leader
	// * raftvote - replies to vote requests in an leadership election
	// * raftsnapshot - installs a snapshot
	tsp := redeoraft.NewTransport(srv, "10.0.0.1:9736", &redeoraft.Config{
		Timeout: time.Minute,
	})
	defer tsp.Close()

	// Use the transport in your raft configuration
	rft, err := raft.NewRaft(raft.DefaultConfig(), &ExampleRaftService{}, raft.NewInmemStore(), raft.NewInmemStore(), raft.NewInmemSnapshotStore(), tsp)
	if err != nil {
		panic(err)
	}
	defer rft.Shutdown()
}

// This example demonstrates the use of sentinel commands on the server
func ExampleSentinel() {
	// Init server
	srv := redeo.NewServer(nil)

	// Start raft
	rft, tsp, err := startRaft(srv)
	if err != nil {
		panic(err)
	}
	defer rft.Shutdown()
	defer tsp.Close()

	// Create a pub-sub broker and handle messages
	broker := redeo.NewPubSubBroker()
	srv.Handle("publish", broker.Publish())
	srv.Handle("subscribe", broker.Subscribe())

	// Listen to sentinel commands
	srv.Handle("sentinel", redeoraft.Sentinel("", rft, broker))

	// $ redis-cli -p 9736 sentinel get-master-addr-by-name mymaster
	// 1) 10.0.0.1
	// 2) 9736
}

// This example demonstrates the use of the leader handler
func ExampleLeader() {
	// Init server
	srv := redeo.NewServer(nil)

	// Start raft
	rft, tsp, err := startRaft(srv)
	if err != nil {
		panic(err)
	}
	defer rft.Shutdown()
	defer tsp.Close()

	// Report leader
	srv.Handle("raftleader", redeoraft.Leader(rft))

	// $ redis-cli -p 9736 raftleader
	// "10.0.0.1:9736"
}

// This example demonstrates the use of the state handler
func ExampleState() {
	// Init server
	srv := redeo.NewServer(nil)

	// Start raft
	rft, tsp, err := startRaft(srv)
	if err != nil {
		panic(err)
	}
	defer rft.Shutdown()
	defer tsp.Close()

	// Report state
	srv.Handle("raftstate", redeoraft.State(rft))

	// $ redis-cli -p 9736 raftstate
	// "leader"
}

// --------------------------------------------------------------------

func startRaft(srv *redeo.Server) (*raft.Raft, *redeoraft.Transport, error) {
	// init transport
	tsp := redeoraft.NewTransport(srv, "10.0.0.1:9736", &redeoraft.Config{
		Timeout: time.Minute,
	})

	// connect raft
	rft, err := raft.NewRaft(raft.DefaultConfig(), &ExampleRaftService{}, raft.NewInmemStore(), raft.NewInmemStore(), raft.NewInmemSnapshotStore(), tsp)
	if err != nil {
		_ = tsp.Close()
		return nil, nil, err
	}

	return rft, tsp, nil
}

type ExampleRaftService struct{}

func (s *ExampleRaftService) Apply(_ *raft.Log) interface{} { return nil }
func (s *ExampleRaftService) Restore(_ io.ReadCloser) error { return nil }
func (s *ExampleRaftService) Snapshot() (raft.FSMSnapshot, error) {
	return nil, raft.ErrNothingNewToSnapshot
}
