package redeoraft_test

import (
	"github.com/hashicorp/raft"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Sentinel", func() {

	It("should reject bad sub-commands", func() {
		peer, _ := testScenario.Leader()

		cn, err := peer.pool.Get()
		Expect(err).NotTo(HaveOccurred())
		defer cn.Close()

		cn.WriteCmdString("SENTINEL")
		Expect(cn.Flush()).To(Succeed())
		Expect(readResponse(cn)).To(Equal("ERR wrong number of arguments for 'SENTINEL' command"))

		cn.WriteCmdString("SENTINEL", "BOGUS")
		Expect(cn.Flush()).To(Succeed())
		Expect(readResponse(cn)).To(Equal("ERR Unknown sentinel subcommand 'BOGUS'"))
	})

	It("should GET-MASTER-ADDR-BY-NAME", func() {
		peer, _ := testScenario.Follower()
		leader, _ := testScenario.Leader()

		cn, err := peer.pool.Get()
		Expect(err).NotTo(HaveOccurred())
		defer cn.Close()

		cn.WriteCmdString("SENTINEL", "GET-MASTER-ADDR-BY-NAME")
		Expect(cn.Flush()).To(Succeed())
		Expect(readResponse(cn)).To(Equal("ERR wrong number of arguments for 'SENTINEL GET-MASTER-ADDR-BY-NAME'"))

		cn.WriteCmdString("SENTINEL", "GET-MASTER-ADDR-BY-NAME", "bogus")
		Expect(cn.Flush()).To(Succeed())
		Expect(readResponse(cn)).To(Equal("<nil>"))

		cn.WriteCmdString("SENTINEL", "GET-MASTER-ADDR-BY-NAME", "MyMaster")
		Expect(cn.Flush()).To(Succeed())
		Expect(readResponse(cn)).To(Equal([]string{"127.0.0.1", leader.Port()}))
	})

	It("should SENTINELS", func() {
		peer, _ := testScenario.Follower()

		cn, err := peer.pool.Get()
		Expect(err).NotTo(HaveOccurred())
		defer cn.Close()

		cn.WriteCmdString("SENTINEL", "sentinels")
		Expect(cn.Flush()).To(Succeed())
		Expect(readResponse(cn)).To(Equal("ERR wrong number of arguments for 'SENTINEL sentinels'"))

		cn.WriteCmdString("SENTINEL", "SENTINELS", "bogus")
		Expect(cn.Flush()).To(Succeed())
		Expect(readResponse(cn)).To(Equal("ERR No such master with that name"))

		cn.WriteCmdString("SENTINEL", "SENTINELS", "myMaster")
		Expect(cn.Flush()).To(Succeed())
		Expect(cn.ReadArrayLen()).To(Equal(5))

		exp := make([][]string, 5)
		for i, p := range testScenario.peers {
			exp[i] = []string{
				"name", "127.0.0.1:" + p.Port(),
				"runid", p.ID(),
				"ip", "127.0.0.1",
				"port", p.Port(),
			}
		}
		Expect(readResponseSlice(5, cn)).To(ConsistOf(exp))
	})

	It("should MASTER", func() {
		peer, _ := testScenario.Follower()
		leader, _ := testScenario.Leader()

		cn, err := peer.pool.Get()
		Expect(err).NotTo(HaveOccurred())
		defer cn.Close()

		cn.WriteCmdString("SENTINEL", "MASTER")
		Expect(cn.Flush()).To(Succeed())
		Expect(readResponse(cn)).To(Equal("ERR wrong number of arguments for 'SENTINEL MASTER'"))

		cn.WriteCmdString("SENTINEL", "master", "bogus")
		Expect(cn.Flush()).To(Succeed())
		Expect(readResponse(cn)).To(Equal("ERR No such master with that name"))

		cn.WriteCmdString("SENTINEL", "master", "myMaster")
		Expect(cn.Flush()).To(Succeed())
		Expect(readResponse(cn)).To(Equal([]string{
			"name", "mymaster",
			"ip", "127.0.0.1",
			"port", leader.Port(),
			"runid", leader.ID(),
			"flags", "master",
			"role-reported", "master",
			"num-slaves", "4",
			"num-other-sentinels", "4",
		}))
	})

	It("should SLAVES", func() {
		peer, _ := testScenario.Follower()
		leader, _ := testScenario.Leader()

		cn, err := peer.pool.Get()
		Expect(err).NotTo(HaveOccurred())
		defer cn.Close()

		cn.WriteCmdString("SENTINEL", "SLAVES")
		Expect(cn.Flush()).To(Succeed())
		Expect(readResponse(cn)).To(Equal("ERR wrong number of arguments for 'SENTINEL SLAVES'"))

		cn.WriteCmdString("SENTINEL", "slaves", "bogus")
		Expect(cn.Flush()).To(Succeed())
		Expect(readResponse(cn)).To(Equal("ERR No such master with that name"))

		cn.WriteCmdString("SENTINEL", "slaves", "myMaster")
		Expect(cn.Flush()).To(Succeed())
		Expect(cn.ReadArrayLen()).To(Equal(4))

		exp := make([][]string, 0, 4)
		for _, p := range testScenario.peers {
			if p.State() == raft.Leader {
				continue
			}
			exp = append(exp, []string{
				"name", "mymaster",
				"ip", "127.0.0.1",
				"port", p.Port(),
				"runid", p.ID(),
				"flags", "slave",
				"role-reported", "slave",
				"master-host", "127.0.0.1",
				"master-port", leader.Port(),
			})
		}
		Expect(readResponseSlice(4, cn)).To(ConsistOf(exp))
	})

})
