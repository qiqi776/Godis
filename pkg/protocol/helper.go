package protocol

import (
	"fmt"
	"strings"
)

// 将空格分隔的命令字符串转换为RESP协议字节切片
func EncodeCmd(commandLine string) []byte {
	parts := strings.Fields(commandLine)
	if len(parts) == 0 {
		return []byte{}
	}
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(parts)))
	for _, part := range parts {
		sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(part), part))
	}
	return []byte(sb.String())
}