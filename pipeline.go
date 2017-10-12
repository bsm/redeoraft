package redeoraft

import (
	"sync/atomic"
	"time"

	"github.com/bsm/redeo/client"
	"github.com/hashicorp/raft"
)

type pipeline struct {
	trans *Transport

	cn   client.Conn
	pool *client.Pool

	done     chan raft.AppendFuture
	shutdown chan struct{}
	closed   int32
}

func newPipeline(trans *Transport, target raft.ServerAddress) (*pipeline, error) {
	pool, err := trans.fetchPool(target)
	if err != nil {
		return nil, err
	}

	cn, err := pool.Get()
	if err != nil {
		return nil, err
	}

	return &pipeline{
		trans: trans,

		cn:   cn,
		pool: pool,

		done:     make(chan raft.AppendFuture, 128),
		shutdown: make(chan struct{}),
	}, nil
}

func (p *pipeline) write(req *raft.AppendEntriesRequest) error {
	return p.trans.writeRPC(p.cn, p.trans.conf.AppendEntriesCommand, req)
}

func (p *pipeline) recv(res *raft.AppendEntriesResponse) error {
	if err := p.trans.flushRPC(p.cn); err != nil {
		return err
	}
	return p.trans.readRPC(p.cn, res)
}

func (p *pipeline) AppendEntries(req *raft.AppendEntriesRequest, res *raft.AppendEntriesResponse) (raft.AppendFuture, error) {
	start := time.Now()
	if err := p.write(req); err != nil {
		return nil, err
	}

	future := &pipeFuture{
		recv:  p.recv,
		start: start,
		req:   req,
		res:   res,
	}

	select {
	case p.done <- future:
		return future, nil
	case <-p.shutdown:
		return nil, raft.ErrPipelineShutdown
	}
}

func (p *pipeline) Consumer() <-chan raft.AppendFuture { return p.done }

func (p *pipeline) Close() error {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil
	}

	close(p.shutdown)
	p.pool.Put(p.cn)
	return nil
}

// --------------------------------------------------------------------

// pipeFuture is used for waiting on a pipelined append
// entries RPC.
type pipeFuture struct {
	recv func(*raft.AppendEntriesResponse) error
	done bool

	start time.Time
	req   *raft.AppendEntriesRequest
	res   *raft.AppendEntriesResponse
	err   error
}

func (f *pipeFuture) apply() {
	if !f.done {
		f.err = f.recv(f.res)
		f.done = true
	}
}

func (f *pipeFuture) Start() time.Time                    { return f.start }
func (f *pipeFuture) Request() *raft.AppendEntriesRequest { return f.req }
func (f *pipeFuture) Response() *raft.AppendEntriesResponse {
	f.apply()
	return f.res
}
func (f *pipeFuture) Error() error {
	f.apply()
	return f.err
}
