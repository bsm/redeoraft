package redeoraft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsm/redeo"
	"github.com/bsm/redeo/client"
	"github.com/bsm/redeo/resp"
	"github.com/hashicorp/raft"
)

var (
	errInvalidResponse = errors.New("invalid response")
)

var (
	typeOfAppendEntriesResponse   = reflect.TypeOf(&raft.AppendEntriesResponse{})
	typeOfRequestVoteResponse     = reflect.TypeOf(&raft.RequestVoteResponse{})
	typeOfInstallSnapshotResponse = reflect.TypeOf(&raft.InstallSnapshotResponse{})
)

var _ raft.Transport = (*Transport)(nil)

// Options allow to customise transports
type Options struct {
	// AppendEntriesCommand allows to customise the
	// command name which is used to append entries.
	// Default: raftappend
	AppendEntriesCommand string

	// RequestVoteCommand allows to customise the
	// command name which is used to request a vote.
	// Default: raftvote
	RequestVoteCommand string

	// InstallSnapshotCommand allows to customise the
	// command name which is used to install a snapshot.
	// Default: raftsnapshot
	InstallSnapshotCommand string

	// Timeout is used to apply I/O deadlines.
	// Default: 0 (= no timeout)
	Timeout time.Duration
}

func (o *Options) norm() {
	if o.AppendEntriesCommand == "" {
		o.AppendEntriesCommand = "raftappend"
	}
	if o.RequestVoteCommand == "" {
		o.RequestVoteCommand = "raftvote"
	}
	if o.InstallSnapshotCommand == "" {
		o.InstallSnapshotCommand = "raftsnapshot"
	}
}

// Transport allows redeo instances to communicate cluster messages
type Transport struct {
	addr    raft.ServerAddress
	opt     *Options
	consume chan raft.RPC

	clients   map[raft.ServerAddress]*client.Pool
	clientsMu sync.RWMutex

	heartbeatFn func(raft.RPC)
	heartbeatMu sync.Mutex

	closed     int32
	shutdown   chan struct{}
	bufferPool sync.Pool
}

// NewTransport creates a new transport and installs the required handlers on the server (see Options).
// It also requires an address it can advertise to peers.
func NewTransport(s *redeo.Server, advertise raft.ServerAddress, opt *Options) *Transport {
	if opt == nil {
		opt = new(Options)
	}
	opt.norm()

	t := &Transport{
		addr:     advertise,
		opt:      opt,
		clients:  make(map[raft.ServerAddress]*client.Pool),
		consume:  make(chan raft.RPC),
		shutdown: make(chan struct{}),
	}

	s.HandleStreamFunc(t.opt.AppendEntriesCommand, func(w resp.ResponseWriter, c *resp.CommandStream) {
		t.handle(w, c, 1, new(raft.AppendEntriesRequest), typeOfAppendEntriesResponse)
	})
	s.HandleStreamFunc(t.opt.RequestVoteCommand, func(w resp.ResponseWriter, c *resp.CommandStream) {
		t.handle(w, c, 1, new(raft.RequestVoteRequest), typeOfRequestVoteResponse)
	})
	s.HandleStreamFunc(t.opt.InstallSnapshotCommand, func(w resp.ResponseWriter, c *resp.CommandStream) {
		t.handle(w, c, 2, new(raft.InstallSnapshotRequest), typeOfInstallSnapshotResponse)
	})

	return t
}

// Consumer implements the raft.Transport interface.
func (t *Transport) Consumer() <-chan raft.RPC { return t.consume }

// LocalAddr implements the raft.Transport interface.
func (t *Transport) LocalAddr() raft.ServerAddress { return t.addr }

// EncodePeer implements the raft.Transport interface.
func (t *Transport) EncodePeer(peer raft.ServerAddress) []byte { return []byte(peer) }

// DecodePeer implements the raft.Transport interface.
func (t *Transport) DecodePeer(peer []byte) raft.ServerAddress { return raft.ServerAddress(peer) }

// SetHeartbeatHandler implements the raft.Transport interface.
func (t *Transport) SetHeartbeatHandler(fn func(rpc raft.RPC)) {
	t.heartbeatMu.Lock()
	t.heartbeatFn = fn
	t.heartbeatMu.Unlock()
}

// AppendEntriesPipeline returns an interface that can be used to pipeline
// AppendEntries requests.
func (t *Transport) AppendEntriesPipeline(target raft.ServerAddress) (raft.AppendPipeline, error) {
	if 1 == 1 {
		return nil, raft.ErrPipelineReplicationNotSupported
	}
	if t.isClosed() {
		return nil, raft.ErrTransportShutdown
	}
	return newPipeline(t, target)
}

// AppendEntries implements the Transport interface.
func (t *Transport) AppendEntries(target raft.ServerAddress, req *raft.AppendEntriesRequest, res *raft.AppendEntriesResponse) error {
	return t.callRPC(target, t.opt.AppendEntriesCommand, req, res)
}

// RequestVote implements the Transport interface.
func (t *Transport) RequestVote(target raft.ServerAddress, req *raft.RequestVoteRequest, res *raft.RequestVoteResponse) error {
	return t.callRPC(target, t.opt.RequestVoteCommand, req, res)
}

// InstallSnapshot implements the Transport interface.
func (t *Transport) InstallSnapshot(target raft.ServerAddress, req *raft.InstallSnapshotRequest, res *raft.InstallSnapshotResponse, snap io.Reader) error {
	return t.withConn(target, func(cn client.Conn) error {
		if err := t.writeRPCStream(cn, t.opt.InstallSnapshotCommand, req, snap, req.Size); err != nil {
			return err
		}
		return t.readRPC(cn, res)
	})
}

// Close closes the transport and abandons calls in progress
func (t *Transport) Close() error {
	if !atomic.CompareAndSwapInt32(&t.closed, 0, 1) {
		return nil
	}

	close(t.shutdown)

	var err error
	t.clientsMu.Lock()
	defer t.clientsMu.Unlock()

	for target, c := range t.clients {
		if e := c.Close(); e != nil {
			err = e
		}
		delete(t.clients, target)
	}
	return err
}

// --------------------------------------------------------------------

func (t *Transport) isClosed() bool {
	return atomic.LoadInt32(&t.closed) == 1
}

func (t *Transport) withConn(target raft.ServerAddress, fn func(cn client.Conn) error) error {
	if t.isClosed() {
		return raft.ErrTransportShutdown
	}

	pool, err := t.fetchPool(target)
	if err != nil {
		return err
	}

	cn, err := pool.Get()
	if err != nil {
		return err
	}
	defer pool.Put(cn)

	return fn(cn)
}

func (t *Transport) callRPC(target raft.ServerAddress, cmd string, req, res interface{}) error {
	return t.withConn(target, func(cn client.Conn) error {
		if err := t.writeRPC(cn, cmd, req); err != nil {
			return err
		}
		if err := t.flushRPC(cn); err != nil {
			return err
		}
		return t.readRPC(cn, res)
	})
}

func (t *Transport) writeRPC(cn client.Conn, cmd string, req interface{}) error {
	buf := t.fetchBuffer()
	defer t.bufferPool.Put(buf)

	// encode request
	if err := gob.NewEncoder(buf).Encode(req); err != nil {
		return err
	}

	// write command
	cn.WriteCmd(cmd, buf.Bytes())
	return nil
}

func (t *Transport) flushRPC(cn client.Conn) error {
	// set timeout
	if timeout := t.opt.Timeout; timeout > 0 {
		_ = cn.SetWriteDeadline(time.Now().Add(timeout))
	}

	// flush command
	if err := cn.Flush(); err != nil {
		cn.MarkFailed()
		return err
	}
	return nil
}

func (t *Transport) writeRPCStream(cn client.Conn, cmd string, req interface{}, src io.Reader, size int64) error {
	buf := t.fetchBuffer()
	defer t.bufferPool.Put(buf)

	// encode request
	if err := gob.NewEncoder(buf).Encode(req); err != nil {
		return err
	}

	// set a deadline, scaled by request size
	if timeout := t.opt.Timeout; timeout > 0 {
		if scale := time.Duration(size / int64(raft.DefaultTimeoutScale)); scale > 1 {
			timeout = timeout * scale
		}
		_ = cn.SetWriteDeadline(time.Now().Add(timeout))
	}

	// write command with stream
	if err := cn.WriteMultiBulkSize(3); err != nil {
		cn.MarkFailed()
		return err
	}
	cn.WriteBulkString(cmd)
	cn.WriteBulk(buf.Bytes())

	if err := cn.CopyBulk(src, size); err != nil {
		cn.MarkFailed()
		return err
	}

	// flush any remaining bytes
	if err := cn.Flush(); err != nil {
		cn.MarkFailed()
		return err
	}

	return nil
}
func (t *Transport) readRPC(cn client.Conn, res interface{}) error {
	// set timeout
	if timeout := t.opt.Timeout; timeout > 0 {
		_ = cn.SetReadDeadline(time.Now().Add(timeout))
	}

	// check response
	typ, err := cn.PeekType()
	if err != nil {
		cn.MarkFailed()
		return err
	}

	switch typ {
	case resp.TypeError:
		msg, err := cn.ReadError()
		if err != nil {
			return err
		}
		return fmt.Errorf(msg)
	case resp.TypeBulk:
		src, err := cn.StreamBulk()
		if err != nil {
			cn.MarkFailed()
			return err
		}
		return gob.NewDecoder(src).Decode(res)
	default:
		cn.MarkFailed()
		return errInvalidResponse
	}
}

func (t *Transport) handle(w resp.ResponseWriter, c *resp.CommandStream, requiredArgs int, req interface{}, restp reflect.Type) {
	if c.ArgN() != requiredArgs {
		w.AppendError(redeo.WrongNumberOfArgs(c.Name))
		return
	}

	src, err := c.NextArg()
	if err != nil {
		log.Printf("RAFT Argument Error: %q", err.Error())
		w.AppendErrorf("RAFT Argument Error: %q", err.Error())
		return
	}

	if err := gob.NewDecoder(src).Decode(req); err != nil {
		log.Printf("RAFT Decoding Error: %q", err.Error())
		w.AppendErrorf("RAFT Decoding Error: %q", err.Error())
		return
	}

	var snap io.Reader
	if requiredArgs == 2 {
		snap, err = c.NextArg()
		if err != nil {
			log.Printf("RAFT Argument Error: %q", err.Error())
			w.AppendErrorf("RAFT Argument Error: %q", err.Error())
			return
		}
	}

	res, err := t.syncRaft(req, snap)
	if err != nil {
		log.Printf("RAFT sync error: %s", err.Error())
		w.AppendError("RAFT " + err.Error())
		return
	}

	if reflect.TypeOf(res) != restp {
		log.Printf("RAFT unexpected response: %T", res)
		w.AppendError("RAFT unexpected response")
		return
	}

	dst := t.fetchBuffer()
	defer t.bufferPool.Put(dst)

	if err := gob.NewEncoder(dst).Encode(res); err != nil {
		log.Printf("RAFT Encoding Error: %q", err.Error())
		w.AppendErrorf("RAFT Encoding Error: %q", err.Error())
		return
	}

	w.AppendBulk(dst.Bytes())
}

func (t *Transport) syncRaft(req interface{}, rd io.Reader) (interface{}, error) {
	var callback func(raft.RPC)

	// Check if this is a heartbeat
	if aer, ok := req.(*raft.AppendEntriesRequest); ok &&
		aer.Term != 0 && aer.Leader != nil &&
		aer.PrevLogEntry == 0 && aer.PrevLogTerm == 0 &&
		len(aer.Entries) == 0 && aer.LeaderCommitIndex == 0 {

		t.heartbeatMu.Lock()
		callback = t.heartbeatFn
		t.heartbeatMu.Unlock()
	}

	respChan := make(chan raft.RPCResponse, 1)
	rpc := raft.RPC{
		Command:  req,
		RespChan: respChan,
		Reader:   rd,
	}

	if callback != nil {
		callback(rpc)
	} else {
		select {
		case t.consume <- rpc:
		case <-t.shutdown:
			return nil, raft.ErrTransportShutdown
		}
	}

	select {
	case res := <-respChan:
		return res.Response, res.Error
	case <-t.shutdown:
		return nil, raft.ErrTransportShutdown
	}
}

func (t *Transport) fetchPool(target raft.ServerAddress) (*client.Pool, error) {
	t.clientsMu.RLock()
	c, ok := t.clients[target]
	t.clientsMu.RUnlock()
	if ok {
		return c, nil
	}

	t.clientsMu.Lock()
	defer t.clientsMu.Unlock()

	if c, ok = t.clients[target]; ok {
		return c, nil
	}

	if t.isClosed() {
		return nil, raft.ErrTransportShutdown
	}
	c, err := client.New(nil, func() (net.Conn, error) {
		return net.DialTimeout("tcp", string(target), t.opt.Timeout)
	})
	if err != nil {
		return nil, err
	}

	t.clients[target] = c
	return c, nil
}

func (t *Transport) fetchBuffer() *bytes.Buffer {
	if v := t.bufferPool.Get(); v != nil {
		r := v.(*bytes.Buffer)
		r.Reset()
		return r
	}
	return new(bytes.Buffer)
}
