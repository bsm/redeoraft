package redeoraft_test

import (
	"fmt"
	"testing"

	"github.com/bsm/redeo/client"
	"github.com/hashicorp/raft"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("RedeoRaft", func() {

	var readKey = func(cn client.Conn, key string) (string, error) {
		cn.WriteCmdString("GET", key)
		if err := cn.Flush(); err != nil {
			return "", err
		}

		var v interface{}
		if err := cn.Scan(&v); err != nil {
			return "", err
		}
		return fmt.Sprintf("%v", v), nil
	}

	var expectFollowerKV = func(p *peer, key, value string) {
		if state := p.State(); state == raft.Leader {
			return
		} else {
			Expect(state).To(Equal(raft.Follower))
		}

		cn, err := p.pool.Get()
		Expect(err).NotTo(HaveOccurred())
		defer cn.Close()

		Eventually(func() (string, error) {
			return readKey(cn, "key")
		}, "30s", "1s").Should(Equal(value))
	}

	It("should ping", func() {
		peer, _ := testScenario.Leader()
		cn, err := peer.pool.Get()
		Expect(err).NotTo(HaveOccurred())
		defer cn.Close()

		cn.WriteCmd("PING")
		Expect(cn.Flush()).To(Succeed())
		Expect(scanString(cn)).To(Equal("PONG"))
	})

	It("should query RAFT leader", func() {
		peer, addr := testScenario.Leader()
		cn, err := peer.pool.Get()
		Expect(err).NotTo(HaveOccurred())
		defer cn.Close()

		cn.WriteCmd("RAFTLEADER")
		Expect(cn.Flush()).To(Succeed())
		Expect(scanString(cn)).To(Equal(addr.String()))

		// ensure everything was read
		Expect(cn.UnreadBytes()).To(BeZero())
	})

	It("should query RAFT state", func() {
		peer, _ := testScenario.Leader()
		cn, err := peer.pool.Get()
		Expect(err).NotTo(HaveOccurred())
		defer cn.Close()

		cn.WriteCmd("RAFTSTATE")
		Expect(cn.Flush()).To(Succeed())
		Expect(scanString(cn)).To(Equal("leader"))

		// ensure everything was read
		Expect(cn.UnreadBytes()).To(BeZero())
	})

	It("should query RAFT stats", func() {
		peer, _ := testScenario.Leader()
		cn, err := peer.pool.Get()
		Expect(err).NotTo(HaveOccurred())
		defer cn.Close()

		cn.WriteCmd("RAFTSTATS")
		Expect(cn.Flush()).To(Succeed())

		var v []interface{}
		Expect(cn.Scan(&v)).To(Succeed())
		Expect(v).To(Equal([]interface{}{
			"state", "leader",
			"term", int64(2),
			"num_peers", int64(4),

			"last_log_index", int64(2),
			"last_log_term", int64(2),
			"commit_index", int64(2),
			"applied_index", int64(2),
			"fsm_pending", int64(0),
			"last_snapshot_index", int64(0),
			"last_snapshot_term", int64(0),

			"protocol_version", int64(3),
			"protocol_version_min", int64(0),
			"protocol_version_max", int64(3),
			"snapshot_version_min", int64(0),
			"snapshot_version_max", int64(1),

			"last_contact", int64(0),
		}))

		// ensure everything was read
		Expect(cn.UnreadBytes()).To(BeZero())
	})

	It("should query RAFT peers", func() {
		peer, _ := testScenario.Leader()
		cn, err := peer.pool.Get()
		Expect(err).NotTo(HaveOccurred())
		defer cn.Close()

		var exp, act [][]string
		for _, p := range testScenario.peers {
			exp = append(exp, []string{
				"id", p.ID(),
				"host", "127.0.0.1",
				"port", p.Port(),
				"suffrage", "voter",
			})
		}
		cn.WriteCmd("RAFTPEERS")
		Expect(cn.Flush()).To(Succeed())
		Expect(cn.Scan(&act)).To(Succeed())
		Expect(act).To(Equal(exp))

		// ensure everything was read
		Expect(cn.UnreadBytes()).To(BeZero())
	})

	It("should accept and propagate writes", func() {
		leader, _ := testScenario.Leader()
		Expect(leader).NotTo(BeNil())

		follower, _ := testScenario.Follower()
		Expect(follower).NotTo(BeNil())

		cn1, err := leader.pool.Get()
		Expect(err).NotTo(HaveOccurred())
		defer cn1.Close()

		cn2, err := follower.pool.Get()
		Expect(err).NotTo(HaveOccurred())
		defer cn2.Close()

		// write and read the same key on leader
		cn1.WriteCmdString("SET", "key", "v1")
		cn1.WriteCmdString("GET", "key")
		Expect(cn1.Flush()).To(Succeed())

		// expect "OK" on SET
		Expect(cn1.ReadInlineString()).To(Equal("OK"))

		// expect "v1" on GET
		Expect(readKey(cn1, "key")).To(Equal("v1"))

		// read the key on follower, expect nil
		Expect(readKey(cn2, "key")).To(Equal("<nil>"))

		// wait for propagation, expect follower to return "v1"
		Eventually(func() (string, error) {
			return readKey(cn2, "key")
		}, "10s").Should(Equal("v1"))

		// ensure everything was read
		Expect(cn1.UnreadBytes()).To(BeZero())
		Expect(cn2.UnreadBytes()).To(BeZero())
	})

	It("should propagate to all followers", func() {
		leader, _ := testScenario.Leader()
		Expect(leader).NotTo(BeNil())

		lcn, err := leader.pool.Get()
		Expect(err).NotTo(HaveOccurred())
		defer lcn.Close()

		for i := 0; i < 500; i++ {
			lcn.WriteCmdString("SET", "key", fmt.Sprintf("v%03d", i))
		}
		Expect(lcn.Flush()).To(Succeed())
		for _, p := range testScenario.peers {
			expectFollowerKV(p, "key", "v499")
		}

		for i := 500; i < 700; i++ {
			lcn.WriteCmdString("SET", "key", fmt.Sprintf("v%03d", i))
		}
		Expect(lcn.Flush()).To(Succeed())
		for _, p := range testScenario.peers {
			expectFollowerKV(p, "key", "v699")
		}

		for i := 700; i < 1000; i++ {
			lcn.WriteCmdString("SET", "key", fmt.Sprintf("v%03d", i))
		}
		Expect(lcn.Flush()).To(Succeed())
		for _, p := range testScenario.peers {
			expectFollowerKV(p, "key", "v999")
		}

		for i := 1000; i < 10000; i++ {
			lcn.WriteCmdString("SET", "key", fmt.Sprintf("v%03d", i))
		}
		Expect(lcn.Flush()).To(Succeed())
		for _, p := range testScenario.peers {
			expectFollowerKV(p, "key", "v9999")
		}
	})

})

// --------------------------------------------------------------------

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "redeoraft")
}

var _ = BeforeSuite(func() {
	var err error
	testScenario, err = createScenario(5)
	Expect(err).NotTo(HaveOccurred())

	go func() {
		defer GinkgoRecover()
		testScenario.Run()
	}()

	Eventually(testScenario.HasLeader, "10s").Should(BeTrue())
})

var _ = AfterSuite(func() {
	if testScenario != nil {
		testScenario.Shutdown()
	}
})

func scanString(cn client.Conn) (string, error) {
	var s string
	err := cn.Scan(&s)
	return s, err
}
