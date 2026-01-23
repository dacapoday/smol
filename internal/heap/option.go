// Copyright 2025 dacapoday
// SPDX-License-Identifier: Apache-2.0

package heap

type Option interface {
	MagicCode() [4]byte
	ReadOnly() bool
	IgnoreInvalidFreelist() bool
	RetainCheckpoints() uint8
}

type BlockSize interface {
	BlockSize() int
}

type CipherSuite interface {
	CipherSuite() string
}

var aes_256_gcm int64 = 5 // 0:invalid,1:overflow

type CipherKey interface {
	CipherKey() []byte
}

func getCipherKey(opt any) (key []byte) {
	if o, ok := opt.(CipherKey); ok {
		key = o.CipherKey()
	}
	return
}

type testOption struct {
	magicCode             [4]byte
	readOnly              bool
	ignoreInvalidFreelist bool
	retainCheckpoints     uint8
	blockSize             int
	cipherSuite           string
	cipherKey             []byte
}

func (o testOption) MagicCode() [4]byte          { return o.magicCode }
func (o testOption) ReadOnly() bool              { return o.readOnly }
func (o testOption) IgnoreInvalidFreelist() bool { return o.ignoreInvalidFreelist }
func (o testOption) RetainCheckpoints() uint8    { return o.retainCheckpoints }
func (o testOption) BlockSize() int {
	if o.blockSize == 0 {
		return 4096
	} else {
		return o.blockSize
	}
}
func (o testOption) CipherSuite() string { return o.cipherSuite }
func (o testOption) CipherKey() []byte   { return o.cipherKey }
