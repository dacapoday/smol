package heap

import (
	"testing"

	"maps"

	"github.com/stretchr/testify/require"
)

func TestRingBasic(t *testing.T) {
	var ring ring
	ring.capacity = 7
	ring.reset()

	require.True(t, ring.empty())
	require.False(t, ring.full())

	id := ring.shift()
	require.EqualValues(t, 0, id)

	require.True(t, ring.push(11))
	require.True(t, ring.push(12))
	require.True(t, ring.push(13))
	require.True(t, ring.push(14))
	require.True(t, ring.push(15))
	require.True(t, ring.push(16))
	require.True(t, ring.push(17))
	require.False(t, ring.push(18))

	require.False(t, ring.empty())
	require.True(t, ring.full())

	require.EqualValues(t, 11, ring.shift())
	require.EqualValues(t, 12, ring.shift())
	require.EqualValues(t, 13, ring.shift())
	require.EqualValues(t, 14, ring.shift())
	require.EqualValues(t, 15, ring.shift())
	require.EqualValues(t, 16, ring.shift())
	require.EqualValues(t, 17, ring.shift())
	require.EqualValues(t, 0, ring.shift())

	require.True(t, ring.push(11))
	require.True(t, ring.push(12))
	require.True(t, ring.push(13))
	require.True(t, ring.push(14))
	require.True(t, ring.push(15))
	require.True(t, ring.push(16))
	require.True(t, ring.push(17))
	require.False(t, ring.push(18))

	require.False(t, ring.empty())
	require.True(t, ring.full())

	require.EqualValues(t, 11, ring.shift())
	require.EqualValues(t, 12, ring.shift())
	require.EqualValues(t, 13, ring.shift())
	require.EqualValues(t, 14, ring.shift())
	require.EqualValues(t, 15, ring.shift())
	require.EqualValues(t, 16, ring.shift())
	require.EqualValues(t, 17, ring.shift())
	require.EqualValues(t, 0, ring.shift())
}

func TestRingEmpty(t *testing.T) {
	var ring ring

	require.True(t, ring.empty())
	require.True(t, ring.full())

	id := ring.shift()
	require.EqualValues(t, 0, id)

	require.False(t, ring.push(18))
}

func TestRingUnshift(t *testing.T) {
	var ring ring
	ring.capacity = 7
	ring.reset()

	require.True(t, ring.unshift(11))
	require.True(t, ring.unshift(12))
	require.True(t, ring.unshift(13))
	require.True(t, ring.unshift(14))
	require.True(t, ring.unshift(15))
	require.True(t, ring.unshift(16))
	require.True(t, ring.unshift(17))
	require.False(t, ring.unshift(18))

	require.False(t, ring.empty())
	require.True(t, ring.full())

	require.EqualValues(t, 17, ring.shift())
	require.EqualValues(t, 16, ring.shift())
	require.EqualValues(t, 15, ring.shift())
	require.EqualValues(t, 14, ring.shift())
	require.EqualValues(t, 13, ring.shift())
	require.EqualValues(t, 12, ring.shift())
	require.EqualValues(t, 11, ring.shift())
	require.EqualValues(t, 0, ring.shift())

	require.True(t, ring.unshift(11))
	require.True(t, ring.unshift(12))
	require.True(t, ring.unshift(13))
	require.True(t, ring.unshift(14))
	require.True(t, ring.unshift(15))
	require.True(t, ring.unshift(16))
	require.True(t, ring.unshift(17))
	require.False(t, ring.unshift(18))

	require.False(t, ring.empty())
	require.True(t, ring.full())

	require.EqualValues(t, 17, ring.shift())
	require.EqualValues(t, 16, ring.shift())
	require.EqualValues(t, 15, ring.shift())
	require.EqualValues(t, 14, ring.shift())
	require.EqualValues(t, 13, ring.shift())
	require.EqualValues(t, 12, ring.shift())
	require.EqualValues(t, 11, ring.shift())
	require.EqualValues(t, 0, ring.shift())
}

func TestRingTopBottom(t *testing.T) {
	var ring ring
	ring.capacity = 7
	ring.reset()

	require.EqualValues(t, 0, ring.top())
	require.EqualValues(t, 0, ring.bottom())

	require.True(t, ring.unshift(11))
	require.EqualValues(t, 11, ring.top())
	require.EqualValues(t, 11, ring.bottom())
	require.True(t, ring.unshift(12))
	require.EqualValues(t, 12, ring.top())
	require.EqualValues(t, 11, ring.bottom())
	require.True(t, ring.unshift(13))
	require.EqualValues(t, 13, ring.top())
	require.EqualValues(t, 11, ring.bottom())
	require.True(t, ring.unshift(14))
	require.EqualValues(t, 14, ring.top())
	require.EqualValues(t, 11, ring.bottom())
	require.True(t, ring.unshift(15))
	require.EqualValues(t, 15, ring.top())
	require.EqualValues(t, 11, ring.bottom())
	require.True(t, ring.unshift(16))
	require.EqualValues(t, 16, ring.top())
	require.EqualValues(t, 11, ring.bottom())
	require.True(t, ring.unshift(17))
	require.EqualValues(t, 17, ring.top())
	require.EqualValues(t, 11, ring.bottom())
	require.False(t, ring.unshift(18))
	require.EqualValues(t, 17, ring.top())
	require.EqualValues(t, 11, ring.bottom())

	require.False(t, ring.empty())
	require.True(t, ring.full())

	require.EqualValues(t, 17, ring.shift())
	require.EqualValues(t, 16, ring.top())
	require.EqualValues(t, 11, ring.bottom())
	require.EqualValues(t, 16, ring.shift())
	require.EqualValues(t, 15, ring.top())
	require.EqualValues(t, 11, ring.bottom())
	require.EqualValues(t, 15, ring.shift())
	require.EqualValues(t, 14, ring.top())
	require.EqualValues(t, 11, ring.bottom())
	require.EqualValues(t, 14, ring.shift())
	require.EqualValues(t, 13, ring.top())
	require.EqualValues(t, 11, ring.bottom())
	require.EqualValues(t, 13, ring.shift())
	require.EqualValues(t, 12, ring.top())
	require.EqualValues(t, 11, ring.bottom())
	require.EqualValues(t, 12, ring.shift())
	require.EqualValues(t, 11, ring.top())
	require.EqualValues(t, 11, ring.bottom())
	require.EqualValues(t, 11, ring.shift())
	require.EqualValues(t, 0, ring.top())
	require.EqualValues(t, 0, ring.bottom())
	require.EqualValues(t, 0, ring.shift())
	require.EqualValues(t, 0, ring.top())
	require.EqualValues(t, 0, ring.bottom())

	require.True(t, ring.push(11))
	require.EqualValues(t, 11, ring.top())
	require.EqualValues(t, 11, ring.bottom())
	require.True(t, ring.push(12))
	require.EqualValues(t, 11, ring.top())
	require.EqualValues(t, 12, ring.bottom())
	require.True(t, ring.push(13))
	require.EqualValues(t, 11, ring.top())
	require.EqualValues(t, 13, ring.bottom())
	require.True(t, ring.push(14))
	require.EqualValues(t, 11, ring.top())
	require.EqualValues(t, 14, ring.bottom())
	require.True(t, ring.push(15))
	require.EqualValues(t, 11, ring.top())
	require.EqualValues(t, 15, ring.bottom())
	require.True(t, ring.push(16))
	require.EqualValues(t, 11, ring.top())
	require.EqualValues(t, 16, ring.bottom())
	require.True(t, ring.push(17))
	require.EqualValues(t, 11, ring.top())
	require.EqualValues(t, 17, ring.bottom())
	require.False(t, ring.push(18))
	require.EqualValues(t, 11, ring.top())
	require.EqualValues(t, 17, ring.bottom())

	require.False(t, ring.empty())
	require.True(t, ring.full())

	require.EqualValues(t, 11, ring.shift())
	require.EqualValues(t, 12, ring.top())
	require.EqualValues(t, 17, ring.bottom())
	require.EqualValues(t, 12, ring.shift())
	require.EqualValues(t, 13, ring.top())
	require.EqualValues(t, 17, ring.bottom())
	require.EqualValues(t, 13, ring.shift())
	require.EqualValues(t, 14, ring.top())
	require.EqualValues(t, 17, ring.bottom())
	require.EqualValues(t, 14, ring.shift())
	require.EqualValues(t, 15, ring.top())
	require.EqualValues(t, 17, ring.bottom())
	require.EqualValues(t, 15, ring.shift())
	require.EqualValues(t, 16, ring.top())
	require.EqualValues(t, 17, ring.bottom())
	require.EqualValues(t, 16, ring.shift())
	require.EqualValues(t, 17, ring.top())
	require.EqualValues(t, 17, ring.bottom())
	require.EqualValues(t, 17, ring.shift())
	require.EqualValues(t, 0, ring.top())
	require.EqualValues(t, 0, ring.bottom())
	require.EqualValues(t, 0, ring.shift())
	require.EqualValues(t, 0, ring.top())
	require.EqualValues(t, 0, ring.bottom())
}

func TestRingFreelist(t *testing.T) {
	var ring ring

	const size = 17

	ring.capacity = size
	ring.reset()

	for i := range BlockID(size) {
		require.True(t, ring.push(i+1))
	}
	ids := maps.Collect(ring.freelist)
	require.Equal(t, size, len(ids))
	for k, v := range ids {
		require.Equal(t, uint32(k)+v, uint32(size))
	}
}
