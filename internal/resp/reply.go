package resp

import (
	"fmt"
	"strconv"
	"strings"
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

func ArrayBulkStrings(values [][]byte) []byte {
	var builder strings.Builder
	builder.WriteString("*")
	builder.WriteString(strconv.Itoa(len(values)))
	builder.WriteString("\r\n")
	for _, value := range values {
		builder.Write(BulkString(value))
	}
	return []byte(builder.String())
}