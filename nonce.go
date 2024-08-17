package dxfuse

// based on:
//   https://www.calhoun.io/creating-random-strings-in-go, and
//   https://github.com/dnanexus/dxWDL/blob/master/src/main/java/com/dnanexus/Nonce.java
//
// Create a nonce according to the dnanexus rules (https://documentation.dnanexus.com/developer/api/nonces)

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

type Nonce struct {
	seed    *rand.Rand
	counter uint64
}

const (
	charset  = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	nonceLen = 32
	maxLen   = 128
)

func NewNonce() *Nonce {
	nano := time.Now().UnixNano()
	source := rand.NewSource(nano)
	return &Nonce{
		seed:    rand.New(source),
		counter: 0,
	}
}

// Create a random nonce, no longer than 128 bytes
func (n *Nonce) String() string {
	b := make([]byte, nonceLen)

	// create a string that contains only readable characters. It can only
	// include characters that can be passed in http requests.
	for i := range b {
		b[i] = charset[n.seed.Intn(len(charset))]
	}

	// add a counter, to better distinguish requests
	cnt := atomic.AddUint64(&n.counter, 1)

	// add the time, for improved randomness
	retval := fmt.Sprintf("%s_%d_%d", string(b), time.Now().UnixNano(), cnt)
	if len(retval) > maxLen {
		// Make sure we don't go over the dnanexus limit
		return retval[0 : maxLen-1]
	}
	return retval
}
