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

	h := &sentinelHandler{Raft: r, Name: name}
	return redeo.SubCommands{
		"get-master-addr-by-name": redeo.HandlerFunc(h.GetMasterAddrByName),
		"sentinels":               redeo.HandlerFunc(h.Sentinels),
		"master":                  redeo.HandlerFunc(h.Master),
		"slaves":                  redeo.HandlerFunc(h.Slaves),
	}
}

type sentinelHandler struct {
	*raft.Raft
	Name string
}

// GetMasterAddrByName handles get-master-addr-by-name sub-command
func (h *sentinelHandler) GetMasterAddrByName(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 1 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}

	if name := c.Arg(0).String(); strings.ToLower(name) != h.Name {
		w.AppendNil()
		return
	}

	ip, port, _ := net.SplitHostPort(string(h.Leader()))
	if ip == "" || port == "" {
		w.AppendNil()
		return
	}

	w.AppendArrayLen(2)
	w.AppendBulkString(ip)
	w.AppendBulkString(port)
}

// Sentinels handles sentinels sub-command
func (h sentinelHandler) Sentinels(w resp.ResponseWriter, c *resp.Command) {
	if ok := h.validateMasterName(w, c); !ok {
		return
	}

	peers, ok := h.validatePeers(w)
	if !ok {
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

// Master handles master sub-command
func (h sentinelHandler) Master(w resp.ResponseWriter, c *resp.Command) {
	if ok := h.validateMasterName(w, c); !ok {
		return
	}

	peers, ok := h.validatePeers(w)
	if !ok {
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

// Slaves handles slaves sub-command
func (h sentinelHandler) Slaves(w resp.ResponseWriter, c *resp.Command) {
	if ok := h.validateMasterName(w, c); !ok {
		return
	}

	peers, ok := h.validatePeers(w)
	if !ok {
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

func (h sentinelHandler) validateMasterName(w resp.ResponseWriter, c *resp.Command) bool {
	if c.ArgN() != 1 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return false
	}
	if name := c.Arg(0).String(); strings.ToLower(name) != h.Name {
		w.AppendError("ERR No such master with that name")
		return false
	}
	return true
}

func (h sentinelHandler) validatePeers(w resp.ResponseWriter) ([]raft.Server, bool) {
	future := h.GetConfiguration()
	if err := future.Error(); err != nil {
		w.AppendError("ERR " + err.Error())
		return nil, false
	}
	srv := future.Configuration().Servers
	if srv == nil {
		w.AppendError("ERR no sentinels found")
		return nil, false
	}
	return srv, true
}
