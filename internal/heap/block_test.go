package heap

import (
	"bytes"
	"slices"
	"testing"

	"github.com/dacapoday/smol/mem"
	"github.com/stretchr/testify/require"
)

func TestBlockOpenClose(t *testing.T) {
	var f mem.File
	file := &f

	var b bytes.Buffer
	buf := &b

	magic := [4]byte{'h', 'e', 'a', 'p'}
	var block block[*mem.File]
	meta0, err := block.init(file, magic, 4096)
	require.NoError(t, err, "block.init")
	require.NotNil(t, meta0, "meta0")

	f.WriteTo(buf)

	err = block.close()
	require.NoError(t, err, "block.close")

	f.ReadFrom(buf)

	meta1, err := block.load(file, magic)
	require.NoError(t, err, "block.load")
	require.NotNil(t, meta1, "meta1")

	require.EqualValues(t, meta0, meta1)

	err = block.close()
	require.NoError(t, err, "block.close")
}

func TestBlockLoadSave(t *testing.T) {
	var f mem.File
	file := &f

	magic := [4]byte{'h', 'e', 'a', 'p'}
	var block block[*mem.File]
	meta0, err := block.init(file, magic, 4096)
	require.NoError(t, err, "block.init")
	require.NotNil(t, meta0, "meta0")

	limit := block.limit
	err = block.grow(100)
	require.NoError(t, err, "block.grow")
	require.Equal(t, uint32(limit+100), block.limit)

	meta1 := *meta0
	meta1.Ckp = 222
	meta1.ID = 12
	err = block.save(&meta1)
	require.NoError(t, err, "block.save")

	meta0.ID = 11
	err = block.save(meta0)
	require.NoError(t, err, "block.save")

	meta0, err = block.meta(12)
	require.NoError(t, err, "block.meta")
	require.NotNil(t, meta0, "meta0")

	require.EqualValues(t, uint32(222), meta0.Ckp)

	err = block.close()
	require.NoError(t, err, "block.close")
}

func TestBlockReadWrite(t *testing.T) {
	var f mem.File
	file := &f

	magic := [4]byte{'h', 'e', 'a', 'p'}
	var block block[*mem.File]
	meta0, err := block.init(file, magic, 4096)
	require.NoError(t, err, "block.init")
	require.NotNil(t, meta0, "meta0")

	bid, err := block.extend()
	require.NoError(t, err, "block.extend")
	require.Equal(t, BlockID(2), bid)

	limit := block.limit
	err = block.grow(100)
	require.NoError(t, err, "block.grow")
	require.Equal(t, uint32(limit+100), block.limit)

	src := []byte("test-write")

	_, err = block.writeAt(src, 49)
	require.NoError(t, err, "block.writeAt")

	err = block.sync()
	require.NoError(t, err, "block.sync")

	buf := make([]byte, 100)
	_, err = block.readAt(buf, 49)
	require.NoError(t, err, "block.readAt")

	require.True(t, slices.Equal(src, buf[:len(src)]))

	file.Close()
}
