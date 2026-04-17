package resp

import "errors"

var (
	ErrEmptyCommand = errors.New("Err empty command")
	ErrProtocol  	= errors.New("Err protocol error")
)