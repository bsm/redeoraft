package redeoraft

import (
	"bytes"
	"net"
	"sort"
	"strings"

	"github.com/bsm/redeo"
	"github.com/bsm/redeo/resp"
	"github.com/hashicorp/raft"
)

// Leader handler retrieves the address of the cluster leader
func Leader(r *raft.Raft) redeo.Handler {
	return redeo.HandlerFunc(func(w resp.ResponseWriter, _ *resp.Command) {
		w.AppendBulkString(string(r.Leader()))
	})
}

// Stats handler retrieves the stats of the cluster
func Stats(r *raft.Raft) redeo.Handler {
	return redeo.HandlerFunc(func(w resp.ResponseWriter, _ *resp.Command) {
		pairs := r.Stats()
		delete(pairs, "latest_configuration")

		keys := make([]string, 0, len(pairs))
		for k := range pairs {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		w.AppendArrayLen(len(pairs) * 2)
		for _, k := range keys {
			w.AppendBulkString(k)
			w.AppendBulkString(strings.ToLower(pairs[k]))
		}
	})
}

// State handler returns the state of the current node
func State(r *raft.Raft) redeo.Handler {
	return redeo.HandlerFunc(func(w resp.ResponseWriter, _ *resp.Command) {
		w.AppendBulkString(strings.ToLower(r.State().String()))
	})
}

// Snapshot handler trigger a snapshot
func Snapshot(r *raft.Raft) redeo.Handler {
	return redeo.HandlerFunc(func(w resp.ResponseWriter, _ *resp.Command) {
		if err := r.Snapshot().Error(); err != nil {
			w.AppendError("ERR " + err.Error())
			return
		}

		w.AppendOK()
	})
}

var nonVoterModifier = []byte("nonvoter")

// AddPeer handler add a voting member to the cluster
func AddPeer(r *raft.Raft) redeo.Handler {
	return redeo.HandlerFunc(func(w resp.ResponseWriter, c *resp.Command) {
		if c.ArgN() < 2 {
			w.AppendError(redeo.WrongNumberOfArgs(c.Name))
			return
		}

		serverID := raft.ServerID(c.Arg(0))
		address := raft.ServerAddress(c.Arg(1))

		var future raft.IndexFuture
		if c.ArgN() == 3 && bytes.Equal(bytes.ToLower(c.Arg(0)), nonVoterModifier) {
			future = r.AddNonvoter(serverID, address, 0, 0)
		} else {
			future = r.AddVoter(serverID, address, 0, 0)
		}
		if err := future.Error(); err != nil {
			w.AppendError("ERR " + err.Error())
			return
		}

		w.AppendOK()
	})
}

// RemovePeer removes a member from the cluster
func RemovePeer(r *raft.Raft) redeo.Handler {
	return redeo.HandlerFunc(func(w resp.ResponseWriter, c *resp.Command) {
		if c.ArgN() != 1 {
			w.AppendError(redeo.WrongNumberOfArgs(c.Name))
			return
		}

		serverID := raft.ServerID(c.Arg(0))
		future := r.RemoveServer(serverID, 0, 0)

		if err := future.Error(); err != nil {
			w.AppendError("ERR " + err.Error())
			return
		}

		w.AppendOK()
	})
}

// Peers handler retrieves a list of peers
func Peers(r *raft.Raft) redeo.Handler {
	return redeo.HandlerFunc(func(w resp.ResponseWriter, c *resp.Command) {
		future := r.GetConfiguration()
		if err := future.Error(); err != nil {
			w.AppendError("ERR " + err.Error())
			return
		}

		config := future.Configuration()
		w.AppendArrayLen(len(config.Servers))
		for _, srv := range config.Servers {
			ip, port, _ := net.SplitHostPort(string(srv.Address))

			w.AppendArrayLen(8)
			w.AppendBulkString("id")
			w.AppendBulkString(string(srv.ID))
			w.AppendBulkString("host")
			w.AppendBulkString(ip)
			w.AppendBulkString("port")
			w.AppendBulkString(port)
			w.AppendBulkString("suffrage")
			w.AppendBulkString(strings.ToLower(srv.Suffrage.String()))
		}
	})
}
