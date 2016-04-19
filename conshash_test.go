package conshash_test

import (
	"fmt"
	"testing"

	"github.com/antlinker/conshash"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestMain(t *testing.T) {

	RegisterFailHandler(Fail)
	RegisterTestingT(t)
	RunSpecs(t, "测试")

}

var _ = Describe("测试算法", func() {
	var (
		servernum = 100
		clientnum = 1000000
	)
	It("测试算法", func() {
		hashinger := conshash.CreateConsistentHashinger(20)
		for i := 0; i < servernum; i++ {
			key := fmt.Sprintf("server%d", i)
			hashinger.Put(key, key)
		}
		Expect(hashinger.Len()).To(BeNumerically("==", servernum))

		ipMap := make(map[string]int, 0)
		for i := 0; i < clientnum; i++ {
			si := fmt.Sprintf("clientid%d", i)
			k, _ := hashinger.Get(si)
			if _, ok := ipMap[k]; ok {
				ipMap[k]++
			} else {
				ipMap[k] = 1
			}
		}
		p := clientnum / servernum
		fc := 0
		for k, v := range ipMap {
			fmt.Println("Node IP:", k, " count:", v)
			fc += (v - p) * (v - p)
		}
		tmp := float64(fc) / float64(clientnum)
		fmt.Println("方差：", tmp)
	})
})
