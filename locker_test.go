package cidlocker

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	coredag "github.com/RTradeLtd/coredag"
	gocid "github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

func newCID(t *testing.T, data string) gocid.Cid {
	mhType, _ := multihash.Names["md5"]
	mhLen := multihash.DefaultLengths[mhType]
	dags, err := coredag.ParseInputs("raw", "raw", strings.NewReader(data), mhType, mhLen)
	if err != nil {
		t.Fatal(err)
	}
	if len(dags) == 0 {
		t.Fatal("no dags found")
	}
	return dags[0].Cid()
}

// basic consistency test
func TestCidLocker(t *testing.T) {
	locker := New()
	cid := newCID(t, fmt.Sprintf("%v", time.Now().UnixNano()))
	// test exists
	if locker.Exists(cid) {
		t.Fatal("cid lock should not exist")
	}

	// test create
	locker.Create(cid)

	// test exists
	if !locker.Exists(cid) {
		t.Fatal("cid lock should exist")
	}

	// test lock
	locker.Lock(cid)

	// test unlock
	locker.Unlock(cid)
}

func TestCidLock_Race_Single_Peer(t *testing.T) {
	locker := New()
	wg := &sync.WaitGroup{}
	testFunc := func(cid gocid.Cid, waitTime time.Duration) {
		defer wg.Done()
		time.Sleep(waitTime)
		// test create
		locker.Create(cid)

		// test lock
		locker.Lock(cid)

		// test exists
		locker.Exists(cid)

		// test unlock
		locker.Unlock(cid)
	}
	cid := newCID(t, fmt.Sprintf("%v", time.Now()))
	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go testFunc(cid, time.Nanosecond+time.Duration(i))
		wg.Add(1)
		go testFunc(cid, 0)
	}
	wg.Wait()
}

func TestCidLock_Race_Many_Cid(t *testing.T) {
	locker := New()
	wg := &sync.WaitGroup{}
	testFunc := func(cid gocid.Cid, waitTime time.Duration) {
		defer wg.Done()
		time.Sleep(waitTime)
		// test create
		locker.Create(cid)

		// test lock
		locker.Lock(cid)

		// test exists
		locker.Exists(cid)

		// test unlock
		locker.Unlock(cid)
	}
	for i := 0; i < 500; i++ {
		cid := newCID(t, fmt.Sprintf("%v+%v", time.Now(), i))
		wg.Add(1)
		go testFunc(cid, time.Nanosecond*10)
		wg.Add(1)
		go testFunc(cid, time.Nanosecond*9)
		wg.Add(1)
		go testFunc(cid, time.Nanosecond*8)
		wg.Add(1)
		go testFunc(cid, time.Nanosecond*7)
	}
	wg.Wait()
}
