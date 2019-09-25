package cidlocker

import (
	"sync"

	gocid "github.com/ipfs/go-cid"
	"github.com/tevino/abool"
)

// CIDLocker is used to block access to resources
// on a per-CID basis, useful when guarding concurrent
// access in systems handling many different CIDs (like reference counting)
//
// The only time this blocks is when creating
// the initial lock, afterwards all writes for a record
// are non-blocking unless another record is first published
// at the same time.
type CIDLocker struct {
	locks map[gocid.Cid]*abool.AtomicBool
	mux   sync.RWMutex
}

// New returns a fresh instance of CIDLocker
func New() *CIDLocker {
	return &CIDLocker{
		locks: make(map[gocid.Cid]*abool.AtomicBool),
	}
}

// Create is like exists, except it
// populates the map if the entry does not exist
func (cl *CIDLocker) Create(cid gocid.Cid) {
	if !cl.Exists(cid) {
		cl.mux.Lock()
		cl.locks[cid] = abool.New()
		cl.mux.Unlock()
	}
}

// Exists check if we have a lock for this cid
func (cl *CIDLocker) Exists(cid gocid.Cid) bool {
	cl.mux.RLock()
	_, exists := cl.locks[cid]
	cl.mux.RUnlock()
	return exists
}

// Lock obtains a lock for the cid
func (cl *CIDLocker) Lock(cid gocid.Cid) {
	cl.Create(cid)
	cl.mux.RLock()
	cl.locks[cid].SetToIf(false, true)
	cl.mux.RUnlock()
}

// Unlock reverts the cid lock
func (cl *CIDLocker) Unlock(cid gocid.Cid) {
	cl.Create(cid)
	cl.mux.RLock()
	cl.locks[cid].SetToIf(true, false)
	cl.mux.RUnlock()
}
