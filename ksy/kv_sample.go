package main

import (
	"fmt"

	"github.com/dacapoday/smol/kv"
)

func main() {
	db, err := kv.Open("kv_sample.kv")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Set([]byte("hello"), []byte("world"))
	if err != nil {
		panic(err)
	}

	for i := range 1000 {
		key := fmt.Appendf(nil, "bk%05dke", i)
		val := fmt.Appendf(nil, "bv%05dve", i)
		err = db.Set(key, val)
		if err != nil {
			panic(err)
		}
	}

	key := []byte("bigkey[")
	for i := range 1000 {
		key = fmt.Appendf(key, "k%05d", i)
	}
	key = append(key, "]bigkey"...)

	err = db.Set(key, []byte("bigkey-val"))
	if err != nil {
		panic(err)
	}

	val := []byte("bigval[")
	for i := range 1000 {
		val = fmt.Appendf(val, "v%05d", i)
	}
	val = append(val, "]bigval"...)

	err = db.Set([]byte("bigval-key"), val)
	if err != nil {
		panic(err)
	}
}
