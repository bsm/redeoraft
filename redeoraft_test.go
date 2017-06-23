package redeoraft_test

import (
	"fmt"
	"testing"

	"github.com/bsm/redeo/client"
	"github.com/bsm/redeo/resp"
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

		typ, err := cn.PeekType()
		if err != nil {
			return "", err
		}

		switch typ {
		case resp.TypeBulk:
			return cn.ReadBulkString()
		case resp.TypeNil:
			return "<nil>", cn.ReadNil()
		}
		return "", fmt.Errorf("expected bulk or nil, but got %d", typ)
	}

	var expectFollowerKV = func(p *peer, key, value string) {
		if state := p.State(); state == raft.Leader {
			return
		} else {
			Expect(state).To(Equal(raft.Follower))
		}

		cn, err := p.pool.Get()
		Expect(err).NotTo(HaveOccurred())
		defer p.pool.Put(cn)

		Eventually(func() string {
			s, err := readKey(cn, "key")
			Expect(err).NotTo(HaveOccurred())
			return s
		}, "5s", "1s").Should(Equal(value))
	}

	It("should ping", func() {
		peer, _ := testScenario.Leader()
		cn, err := peer.pool.Get()
		defer peer.pool.Put(cn)

		cn.WriteCmd("PING")
		Expect(cn.Flush()).To(Succeed())

		str, err := cn.ReadBulkString()
		Expect(err).NotTo(HaveOccurred())
		Expect(str).To(Equal("PONG"))
	})

	It("should query RAFT leader", func() {
		peer, addr := testScenario.Leader()
		cn, err := peer.pool.Get()
		defer peer.pool.Put(cn)

		cn.WriteCmd("RAFTLEADER")
		Expect(cn.Flush()).To(Succeed())

		str, err := cn.ReadBulkString()
		Expect(err).NotTo(HaveOccurred())
		Expect(str).To(Equal(addr.String()))
	})

	It("should query RAFT state", func() {
		peer, _ := testScenario.Leader()
		cn, err := peer.pool.Get()
		defer peer.pool.Put(cn)

		cn.WriteCmd("RAFTSTATE")
		Expect(cn.Flush()).To(Succeed())

		str, err := cn.ReadBulkString()
		Expect(err).NotTo(HaveOccurred())
		Expect(str).To(Equal("leader"))
	})

	It("should query RAFT stats", func() {
		peer, _ := testScenario.Leader()
		cn, err := peer.pool.Get()
		defer peer.pool.Put(cn)

		cn.WriteCmd("RAFTSTATS")
		Expect(cn.Flush()).To(Succeed())

		n, err := cn.ReadArrayLen()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(34))

		stats := make(map[string]string)
		for i := 0; i < n; i += 2 {
			key, err := cn.ReadBulkString()
			Expect(err).NotTo(HaveOccurred())
			val, err := cn.ReadBulkString()
			Expect(err).NotTo(HaveOccurred())

			stats[key] = val
		}
		Expect(stats).To(HaveLen(17))
		Expect(stats).To(HaveKeyWithValue("num_peers", "4"))
		Expect(stats).To(HaveKeyWithValue("protocol_version", "3"))
		Expect(stats).To(HaveKeyWithValue("state", "leader"))
	})

	It("should query RAFT peers", func() {
		peer, _ := testScenario.Leader()
		cn, err := peer.pool.Get()
		defer peer.pool.Put(cn)

		cn.WriteCmd("RAFTPEERS")
		Expect(cn.Flush()).To(Succeed())

		n, err := cn.ReadArrayLen()
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(5))

		for i := 0; i < n; i++ {
			m, err := cn.ReadArrayLen()
			Expect(err).NotTo(HaveOccurred())
			Expect(m).To(Equal(3))

			id, err := cn.ReadBulkString()
			Expect(err).NotTo(HaveOccurred())
			Expect(id).To(MatchRegexp(`N[1-5]`))

			addr, err := cn.ReadBulkString()
			Expect(err).NotTo(HaveOccurred())
			Expect(addr).To(MatchRegexp(`127.0.0.1:\d+`))

			suf, err := cn.ReadBulkString()
			Expect(err).NotTo(HaveOccurred())
			Expect(suf).To(Equal("voter"))
		}
	})

	It("should accept and propagate writes", func() {
		leader, _ := testScenario.Leader()
		Expect(leader).NotTo(BeNil())

		follower, _ := testScenario.Follower()
		Expect(follower).NotTo(BeNil())

		cn1, err := leader.pool.Get()
		Expect(err).NotTo(HaveOccurred())
		defer leader.pool.Put(cn1)

		cn2, err := follower.pool.Get()
		Expect(err).NotTo(HaveOccurred())
		defer follower.pool.Put(cn2)

		// write and read the same key on leader
		cn1.WriteCmdString("SET", "key", "v1")
		cn1.WriteCmdString("GET", "key")
		Expect(cn1.Flush()).To(Succeed())

		// expect "OK" on SET
		str, err := cn1.ReadInlineString()
		Expect(err).NotTo(HaveOccurred())
		Expect(str).To(Equal("OK"))

		// expect "v1" on GET
		str, err = readKey(cn1, "key")
		Expect(err).NotTo(HaveOccurred())
		Expect(str).To(Equal("v1"))

		// read the key on follower, expect nil
		str, err = readKey(cn2, "key")
		Expect(err).NotTo(HaveOccurred())
		Expect(str).To(Equal("<nil>"))

		// wait for propagation, expect follower to return "v1"
		Eventually(func() string {
			s, err := readKey(cn2, "key")
			Expect(err).NotTo(HaveOccurred())
			return s
		}, "5s").Should(Equal("v1"))
	})

	It("should propagate to all followers", func() {
		leader, _ := testScenario.Leader()
		Expect(leader).NotTo(BeNil())

		lcn, err := leader.pool.Get()
		Expect(err).NotTo(HaveOccurred())
		defer leader.pool.Put(lcn)

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
		Expect(testScenario.Close()).To(Succeed())
	}
})
