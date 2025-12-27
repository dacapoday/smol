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
