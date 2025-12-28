// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package heap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQueueBasic(t *testing.T) {
	var q queue

	id := q.shift()
	require.EqualValues(t, 0, id)

	q.push(11)
	q.push(12)
	q.push(13)
	q.push(14)
	q.push(15)
	q.push(16)
	q.push(17)

	require.EqualValues(t, 11, q.shift())
	require.EqualValues(t, 12, q.shift())
	require.EqualValues(t, 13, q.shift())
	require.EqualValues(t, 14, q.shift())
	require.EqualValues(t, 15, q.shift())
	require.EqualValues(t, 16, q.shift())
	require.EqualValues(t, 17, q.shift())
	require.EqualValues(t, 0, q.shift())

	q.push(11)
	q.push(12)
	q.push(13)
	q.push(14)
	q.push(15)
	q.push(16)
	q.push(17)

	require.EqualValues(t, 11, q.shift())
	require.EqualValues(t, 12, q.shift())
	require.EqualValues(t, 13, q.shift())
	require.EqualValues(t, 14, q.shift())
	require.EqualValues(t, 15, q.shift())
	require.EqualValues(t, 16, q.shift())
	require.EqualValues(t, 17, q.shift())
	require.EqualValues(t, 0, q.shift())
}

func TestQueueEmpty(t *testing.T) {
	var q queue

	id := q.shift()
	require.EqualValues(t, 0, id)
}

func TestQueueTopBottom(t *testing.T) {
	var q queue

	require.EqualValues(t, 0, q.top())
	require.EqualValues(t, 0, q.bottom())

	q.unshift(11)
	require.EqualValues(t, 11, q.top())
	require.EqualValues(t, 11, q.bottom())
	q.unshift(12)
	require.EqualValues(t, 12, q.top())
	require.EqualValues(t, 11, q.bottom())
	q.unshift(13)
	require.EqualValues(t, 13, q.top())
	require.EqualValues(t, 11, q.bottom())
	q.unshift(14)
	require.EqualValues(t, 14, q.top())
	require.EqualValues(t, 11, q.bottom())
	q.unshift(15)
	require.EqualValues(t, 15, q.top())
	require.EqualValues(t, 11, q.bottom())
	q.unshift(16)
	require.EqualValues(t, 16, q.top())
	require.EqualValues(t, 11, q.bottom())
	q.unshift(17)
	require.EqualValues(t, 17, q.top())
	require.EqualValues(t, 11, q.bottom())

	q.shift()
	require.EqualValues(t, 16, q.top())
	require.EqualValues(t, 11, q.bottom())
	q.shift()
	require.EqualValues(t, 15, q.top())
	require.EqualValues(t, 11, q.bottom())
	q.shift()
	require.EqualValues(t, 14, q.top())
	require.EqualValues(t, 11, q.bottom())
	q.shift()
	require.EqualValues(t, 13, q.top())
	require.EqualValues(t, 11, q.bottom())
	q.shift()
	require.EqualValues(t, 12, q.top())
	require.EqualValues(t, 11, q.bottom())
	q.shift()
	require.EqualValues(t, 11, q.top())
	require.EqualValues(t, 11, q.bottom())
	q.shift()
	require.EqualValues(t, 0, q.top())
	require.EqualValues(t, 0, q.bottom())
	q.shift()
	require.EqualValues(t, 0, q.top())
	require.EqualValues(t, 0, q.bottom())

	q.push(11)
	require.EqualValues(t, 11, q.top())
	require.EqualValues(t, 11, q.bottom())
	q.push(12)
	require.EqualValues(t, 11, q.top())
	require.EqualValues(t, 12, q.bottom())
	q.push(13)
	require.EqualValues(t, 11, q.top())
	require.EqualValues(t, 13, q.bottom())
	q.push(14)
	require.EqualValues(t, 11, q.top())
	require.EqualValues(t, 14, q.bottom())
	q.push(15)
	require.EqualValues(t, 11, q.top())
	require.EqualValues(t, 15, q.bottom())
	q.push(16)
	require.EqualValues(t, 11, q.top())
	require.EqualValues(t, 16, q.bottom())
	q.push(17)
	require.EqualValues(t, 11, q.top())
	require.EqualValues(t, 17, q.bottom())

	q.shift()
	require.EqualValues(t, 12, q.top())
	require.EqualValues(t, 17, q.bottom())
	q.shift()
	require.EqualValues(t, 13, q.top())
	require.EqualValues(t, 17, q.bottom())
	q.shift()
	require.EqualValues(t, 14, q.top())
	require.EqualValues(t, 17, q.bottom())
	q.shift()
	require.EqualValues(t, 15, q.top())
	require.EqualValues(t, 17, q.bottom())
	q.shift()
	require.EqualValues(t, 16, q.top())
	require.EqualValues(t, 17, q.bottom())
	q.shift()
	require.EqualValues(t, 17, q.top())
	require.EqualValues(t, 17, q.bottom())
	q.shift()
	require.EqualValues(t, 0, q.top())
	require.EqualValues(t, 0, q.bottom())
	q.shift()
	require.EqualValues(t, 0, q.top())
	require.EqualValues(t, 0, q.bottom())
}
