package lsm

import "errors"

var (
	ErrClosed   	  = errors.New("lsm: closed")
	ErrInvalidOptions = errors.New("lsm: invalid options")
	ErrInvalidBatch   = errors.New("lsm: invalid batch")
	ErrInvalidKey 	  = errors.New("lsm: invalid key")
)