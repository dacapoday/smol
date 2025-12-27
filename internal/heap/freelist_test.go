package heap

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFreelist(t *testing.T) {
	var src, dst ring

	src.capacity = uint16(rand.Uint32N(uint32(freelistCapacity(64*1024))) + 1)
	t.Log("capacity:", src.capacity)

	dst.capacity = src.capacity
	src.reset()
	dst.reset()

	require.Equal(t, src.capacity, freelistCapacity(int64(freelistSize(src.capacity))))

	freelist := make(Freelist, freelistSize(src.capacity))

	for i := range src.capacity {
		src.push(BlockID(i + 2))
	}

	ring2freelist(&src, 2, freelist)
	freelist2ring(freelist, &dst, freelist.Count())

	require.Equal(t, src.length, freelist.Count())
	require.Equal(t, BlockID(2), freelist.Prev())
	require.False(t, freelist.invalid())

	for range src.capacity {
		require.Equal(t, src.shift(), dst.shift())
	}
}
