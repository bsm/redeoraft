# Redeo Raft

[![GoDoc](https://godoc.org/github.com/bsm/redeoraft?status.svg)](https://godoc.org/github.com/bsm/redeoraft)
[![Build Status](https://travis-ci.org/bsm/redeoraft.png?branch=master)](https://travis-ci.org/bsm/redeoraft)
[![Go Report Card](https://goreportcard.com/badge/github.com/bsm/redeoraft)](https://goreportcard.com/report/github.com/bsm/redeoraft)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Raft transport implementation for Redeo servers.

## Example

```go
func run() {
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
```

## Dependencies

* [library-v2-stage-one](https://github.com/hashicorp/raft/tree/library-v2-stage-one) branch of Hashicorp's Raft implementation
