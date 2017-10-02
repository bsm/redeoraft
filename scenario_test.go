package redeoraft_test

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/bsm/pool"
	"github.com/bsm/redeo"
	"github.com/bsm/redeo/client"
	"github.com/bsm/redeo/resp"
	"github.com/bsm/redeoraft"
	"github.com/hashicorp/raft"
)

var testScenario *scenario

// --------------------------------------------------------------------

type scenario struct {
	listeners []net.Listener
	peers     []*peer
}

func createScenario(n int) (*scenario, error) {
	s := new(scenario)

	for i := 0; i < n; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			s.Shutdown()
			return nil, err
		}
		s.listeners = append(s.listeners, lis)
	}

	servers := s.Servers()
	for i := 0; i < n; i++ {
		peer, err := newPeer(servers[i].ID, servers[i].Address, servers)
		if err != nil {
			s.Shutdown()
			return nil, err
		}
		s.peers = append(s.peers, peer)
	}
	return s, nil
}

func (s *scenario) Servers() []raft.Server {
	addrs := make([]raft.Server, 0, len(s.peers))
	for i, lis := range s.listeners {
		addrs = append(addrs, raft.Server{
			ID:      raft.ServerID(fmt.Sprintf("N%d", i+1)),
			Address: raft.ServerAddress(lis.Addr().String()),
		})
	}
	return addrs
}

func (s *scenario) HasLeader() bool {
	l, _ := s.Leader()
	return l != nil
}

func (s *scenario) Leader() (*peer, net.Addr) {
	for i, sv := range s.peers {
		if sv.State() == raft.Leader {
			return sv, s.listeners[i].Addr()
		}
	}
	return nil, nil
}

func (s *scenario) Follower() (*peer, net.Addr) {
	for i, sv := range s.peers {
		if sv.State() == raft.Follower {
			return sv, s.listeners[i].Addr()
		}
	}
	return nil, nil
}

func (s *scenario) Run() {
	var wg sync.WaitGroup

	for i := range s.peers {
		wg.Add(1)

		l := s.listeners[i]
		p := s.peers[i]
		go func() {
			defer wg.Done()
			_ = p.Serve(l)
		}()
	}
	wg.Wait()
}

func (s *scenario) Shutdown() {
	for _, sv := range s.peers {
		_ = sv.Close()
	}
	s.peers = s.peers[:0]

	for _, lis := range s.listeners {
		_ = lis.Close()
	}
	s.listeners = s.listeners[:0]
}

// --------------------------------------------------------------------

type peer struct {
	*redeo.Server

	ctrl  *raft.Raft
	pool  *client.Pool
	trans *redeoraft.Transport

	port     string
	serverID raft.ServerID

	data   map[string][]byte
	dataMu sync.RWMutex
}

func newPeer(serverID raft.ServerID, addr raft.ServerAddress, servers []raft.Server) (*peer, error) {
	_, port, err := net.SplitHostPort(string(addr))
	if err != nil {
		return nil, err
	}

	pool, err := client.New(&pool.Options{InitialSize: 1}, func() (net.Conn, error) {
		return net.Dial("tcp", string(addr))
	})
	if err != nil {
		return nil, err
	}

	server := redeo.NewServer(nil)
	trans := redeoraft.NewTransport(server, addr, &redeoraft.Options{
		Timeout: time.Second,
	})
	p := &peer{
		Server:   server,
		pool:     pool,
		trans:    trans,
		port:     port,
		serverID: serverID,
		data:     make(map[string][]byte),
	}

	store := raft.NewInmemStore()
	snaps := raft.NewInmemSnapshotStore()

	conf := raft.DefaultConfig()
	conf.LocalID = serverID
	conf.Logger = log.New(ioutil.Discard, "", 0)
	if testing.Verbose() {
		conf.Logger = log.New(os.Stderr, "["+string(serverID)+"] ", log.Ltime)
	}
	ctrl, err := raft.NewRaft(conf, p, store, store, snaps, p.trans)
	if err != nil {
		_ = p.Close()
		return nil, err
	}
	p.ctrl = ctrl

	if err := ctrl.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil {
		_ = p.Close()
		return nil, err
	}

	p.Server.Handle("ping", redeo.Ping())
	p.Server.HandleFunc("get", p.handleGet)
	p.Server.HandleFunc("set", p.handleSet)
	p.Server.Handle("raftleader", redeoraft.Leader(ctrl))
	p.Server.Handle("raftstats", redeoraft.Stats(ctrl))
	p.Server.Handle("raftstate", redeoraft.State(ctrl))
	p.Server.Handle("raftpeers", redeoraft.Peers(ctrl))

	broker := redeo.NewPubSubBroker()
	p.Server.Handle("sentinel", redeoraft.Sentinel("", ctrl, broker))
	p.Server.Handle("publish", broker.Publish())
	p.Server.Handle("subscribe", broker.Subscribe())

	return p, nil
}

func (p *peer) ID() string            { return string(p.serverID) }
func (p *peer) Port() string          { return p.port }
func (p *peer) State() raft.RaftState { return p.ctrl.State() }

func (p *peer) Close() (err error) {
	if p.ctrl != nil {
		p.ctrl.Shutdown()
	}
	if p.pool != nil {
		_ = p.pool.Close()
	}
	if p.trans != nil {
		_ = p.trans.Close()
	}
	return
}

func (p *peer) Apply(log *raft.Log) interface{} {
	var res fsmApplyResponse
	var cmd fsmCommand
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		res.Err = err
		return &res
	}

	if cmd.Name != "SET" {
		res.Err = fmt.Errorf("unknown command %q", cmd.Name)
		return &res
	}
	p.dataMu.Lock()
	p.data[string(cmd.Key)] = cmd.Value
	p.dataMu.Unlock()
	return &res
}

func (p *peer) Restore(rc io.ReadCloser) error {
	data := make(map[string][]byte)
	err := json.NewDecoder(rc).Decode(&data)
	if err != nil {
		return err
	}

	p.dataMu.Lock()
	p.data = data
	p.dataMu.Unlock()
	return nil
}

func (p *peer) Snapshot() (raft.FSMSnapshot, error) { return p, nil }
func (p *peer) Release()                            {}
func (p *peer) Persist(w raft.SnapshotSink) error {
	defer w.Cancel()

	enc := json.NewEncoder(w)
	p.dataMu.RLock()
	err := enc.Encode(p.data)
	p.dataMu.RUnlock()
	return err
}

func (p *peer) handleGet(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 1 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}

	key := c.Arg(0)

	p.dataMu.RLock()
	val, ok := p.data[string(key)]
	p.dataMu.RUnlock()

	if !ok {
		w.AppendNil()
		return
	}
	w.AppendBulk(val)
}

func (p *peer) handleSet(w resp.ResponseWriter, c *resp.Command) {
	if c.ArgN() != 2 {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}

	cmd, err := json.Marshal(&fsmCommand{
		Name:  "SET",
		Key:   c.Arg(0),
		Value: c.Arg(1),
	})
	if err != nil {
		w.AppendError("ERR " + err.Error())
		return
	}

	res := p.ctrl.Apply(cmd, time.Second)
	if err := res.Error(); err != nil {
		w.AppendError("ERR " + err.Error())
		return
	}

	if err := res.Response().(*fsmApplyResponse).Err; err != nil {
		w.AppendError("ERR " + err.Error())
		return
	}

	w.AppendOK()
}

// --------------------------------------------------------------------

type fsmCommand struct {
	Name       string
	Key, Value []byte
}

type fsmApplyResponse struct {
	Err error
}
