package engine

import "errors"

type Kind uint8

const (
	KindString Kind = iota + 1
	KindList
)

type Entity struct {
	Kind  Kind
	Value any
}

var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

func copyBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	return append([]byte(nil), src...)
}

func copyByteSlices(src [][]byte) [][]byte {
	if len(src) == 0 {
		return nil
	}
	out := make([][]byte, 0, len(src))
	for _, item := range src {
		out = append(out, copyBytes(item))
	}
	return out
}