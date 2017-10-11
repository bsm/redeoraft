package redeoraft

import (
	"net"
	"strconv"
	"strings"

	"github.com/bsm/redeo"
	"github.com/bsm/redeo/resp"
	"github.com/hashicorp/raft"
)

// Sentinel handler respond to a subset of SENTINEL commands and
// makes your server behave like an instance of a sentinel cluster.
//
// Implemented sub-commands are:
//
//     SENTINELS - returns (abbreviated) peer attributes
//     MASTER - returns (abbreviated) master attributes
//     SLAVES - returns (abbreviated) slave attributes
//     GET-MASTER-ADDR-BY-NAME - returns a the master address
func Sentinel(name string, r *raft.Raft, b *redeo.PubSubBroker) redeo.Handler {
	if name == "" {
		name = "mymaster"
	} else {
		name = strings.ToLower(name)
	}

	go func() {
		prevIP, prevPort, _ := net.SplitHostPort(string(r.Leader()))

		for range r.LeaderCh() {
			currIP, currPort, _ := net.SplitHostPort(string(r.Leader()))
			b.PublishMessage("+switch-master", strings.Join([]string{name, prevIP, prevPort, currIP, currPort}, " "))
			prevIP, prevPort = currIP, currPort
		}
	}()

	return sentinelHandler{Raft: r, Name: name}
}

type sentinelHandler struct {
	*raft.Raft
	Name string
}

func (h sentinelHandler) ServeRedeo(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() == 0 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}

	firstArg := c.Arg(0).String()
	switch subCmd := strings.ToLower(firstArg); subCmd {
	case "get-master-addr-by-name":
		if c.ArgN() != 2 {
			w.AppendError("ERR wrong number of arguments for '" + c.Name + " " + firstArg + "'")
			return
		}
		if name := c.Arg(1).String(); strings.ToLower(name) != h.Name {
			w.AppendNil()
			return
		}
		h.masterAddr(w)
	case "sentinels", "master", "slaves":
		if c.ArgN() != 2 {
			w.AppendError("ERR wrong number of arguments for '" + c.Name + " " + firstArg + "'")
			return
		}
		if name := c.Arg(1).String(); strings.ToLower(name) != h.Name {
			w.AppendError("ERR No such master with that name")
			return
		}

		switch subCmd {
		case "sentinels":
			h.sentinels(w)
		case "master":
			h.master(w)
		case "slaves":
			h.slaves(w)
		}
	default:
		w.AppendError("ERR Unknown sentinel subcommand '" + firstArg + "'")
	}
}

func (h sentinelHandler) peersOrError(w resp.ResponseWriter) []raft.Server {
	future := h.GetConfiguration()
	if err := future.Error(); err != nil {
		w.AppendError("ERR " + err.Error())
		return nil
	}
	srv := future.Configuration().Servers
	if srv == nil {
		w.AppendError("ERR no sentinels found")
		return nil
	}
	return srv
}

func (h sentinelHandler) masterAddr(w resp.ResponseWriter) {
	ip, port, _ := net.SplitHostPort(string(h.Leader()))

	w.AppendArrayLen(2)
	w.AppendBulkString(ip)
	w.AppendBulkString(port)
}

func (h sentinelHandler) sentinels(w resp.ResponseWriter) {
	peers := h.peersOrError(w)
	if peers == nil {
		return
	}

	w.AppendArrayLen(len(peers))
	for _, peer := range peers {
		ip, port, _ := net.SplitHostPort(string(peer.Address))

		w.AppendArrayLen(8)
		w.AppendBulkString("name")
		w.AppendBulkString(string(peer.Address))
		w.AppendBulkString("runid")
		w.AppendBulkString(string(peer.ID))
		w.AppendBulkString("ip")
		w.AppendBulkString(ip)
		w.AppendBulkString("port")
		w.AppendBulkString(port)
	}
}

func (h sentinelHandler) master(w resp.ResponseWriter) {
	peers := h.peersOrError(w)
	if peers == nil {
		return
	}

	leader := h.Leader()
	var master *raft.Server
	for _, peer := range peers {
		if peer.Address == leader {
			master = &peer
			break
		}
	}

	if master == nil {
		w.AppendNil()
		return
	}

	ip, port, err := net.SplitHostPort(string(master.Address))
	if err != nil {
		w.AppendError("ERR " + err.Error())
		return
	}

	numSlaves := strconv.Itoa(len(peers) - 1)
	w.AppendArrayLen(16)
	w.AppendBulkString("name")
	w.AppendBulkString(h.Name)
	w.AppendBulkString("ip")
	w.AppendBulkString(ip)
	w.AppendBulkString("port")
	w.AppendBulkString(port)
	w.AppendBulkString("runid")
	w.AppendBulkString(string(master.ID))
	w.AppendBulkString("flags")
	w.AppendBulkString("master")
	w.AppendBulkString("role-reported")
	w.AppendBulkString("master")
	w.AppendBulkString("num-slaves")
	w.AppendBulkString(numSlaves)
	w.AppendBulkString("num-other-sentinels")
	w.AppendBulkString(numSlaves)
}

func (h sentinelHandler) slaves(w resp.ResponseWriter) {
	peers := h.peersOrError(w)
	if peers == nil {
		return
	}

	leader := h.Leader()
	masterIP, masterPort, err := net.SplitHostPort(string(leader))
	if err != nil {
		w.AppendError("ERR " + err.Error())
		return
	}

	slaves := make([]raft.Server, 0, len(peers)-1)
	for _, peer := range peers {
		if peer.Address != leader {
			slaves = append(slaves, peer)
		}
	}

	w.AppendArrayLen(len(slaves))
	for _, slave := range slaves {
		ip, port, _ := net.SplitHostPort(string(slave.Address))

		w.AppendArrayLen(16)
		w.AppendBulkString("name")
		w.AppendBulkString(h.Name)
		w.AppendBulkString("ip")
		w.AppendBulkString(ip)
		w.AppendBulkString("port")
		w.AppendBulkString(port)
		w.AppendBulkString("runid")
		w.AppendBulkString(string(slave.ID))
		w.AppendBulkString("flags")
		w.AppendBulkString("slave")
		w.AppendBulkString("role-reported")
		w.AppendBulkString("slave")
		w.AppendBulkString("master-host")
		w.AppendBulkString(masterIP)
		w.AppendBulkString("master-port")
		w.AppendBulkString(masterPort)
	}

}
