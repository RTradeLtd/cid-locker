package cidlocker

import (
	"sync"

	gocid "github.com/ipfs/go-cid"
	"github.com/tevino/abool"
)

// PeerIDLocker is used to handle mutex lock/unlock
// on a per-peerID basis for systems that may need
// concurrent access to peerID specific resources
// such as IPNS record publishing
//
// The only time this blocks is when creating
// the initial lock, afterwards all writes for a record
// are non-blocking unless another record is first published
// at the same time.
type PeerIDLocker struct {
	locks map[gocid.Cid]*abool.AtomicBool
	mux   sync.RWMutex
}

// New returns a fresh instance of PeerIDLocker
func New() *PeerIDLocker {
	return &PeerIDLocker{
		locks: make(map[gocid.Cid]*abool.AtomicBool),
	}
}

// Create is like exists, except it
// populates the map if the entry does not exist
func (pl *PeerIDLocker) Create(cid gocid.Cid) {
	if !pl.Exists(cid) {
		pl.mux.Lock()
		pl.locks[cid] = abool.New()
		pl.mux.Unlock()
	}
}

// Exists check if we have a lock for this peerID
func (pl *PeerIDLocker) Exists(cid gocid.Cid) bool {
	pl.mux.RLock()
	_, exists := pl.locks[cid]
	pl.mux.RUnlock()
	return exists
}

// Lock obtains a lock for the peerID
func (pl *PeerIDLocker) Lock(cid gocid.Cid) {
	pl.Create(cid)
	pl.mux.RLock()
	pl.locks[cid].SetToIf(false, true)
	pl.mux.RUnlock()
}

// Unlock reverts the peerID lock
func (pl *PeerIDLocker) Unlock(cid gocid.Cid) {
	pl.Create(cid)
	pl.mux.RLock()
	pl.locks[cid].SetToIf(true, false)
	pl.mux.RUnlock()
}
