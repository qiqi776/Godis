package resp

import (
	"fmt"
	"strconv"
)

func SimpleString(value string) []byte {
	return []byte("+" + value + "\r\n")
}

func Error(value string) []byte {
	return []byte("-" + value + "\r\n")
}

func BulkString(value []byte) []byte {
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value))
}

func Integer(value int64) []byte {
    return []byte(":" + strconv.FormatInt(value, 10) + "\r\n")
}

func NullBulkString() []byte {
	return []byte("$-1\r\n")
}