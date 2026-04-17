package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

func ReadCommand(r *bufio.Reader) ([][]byte, error) {
	cmd, err := r.Peek(1)
	if err != nil {
		return nil, err
	}

	switch cmd[0] {
	case '*':
		return readArrayCommand(r)
	default:
		return readInlineCommand(r)
	}
}

func readArrayCommand(r *bufio.Reader) ([][]byte, error) {
	l, err := readLine(r)
	if err != nil {
		return nil, err
	}

	count, err := strconv.Atoi(strings.TrimPrefix(l, "*"))
	if err != nil || count < 0 {
		return nil, fmt.Errorf("%w: invalid multibulk length", ErrProtocol)
	}

	tokens := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		header, err := readLine(r)
		if err != nil {
			return nil, err
		}

		if !strings.HasPrefix(header, "$") {
			return nil, fmt.Errorf("%w: expected bulk string", ErrProtocol)
		}

		length, err := strconv.Atoi(strings.TrimPrefix(header, "$"))
		if err != nil {
			return nil, err
		}

		buf := make([]byte, length+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}

		if buf[length] != '\r' || buf[length+1] != '\n' {
			return nil, fmt.Errorf("%w: invalid bulk terminator", ErrProtocol)
		}

		tokens = append(tokens, append([]byte(nil), buf[:length]...))
	}

	return tokens, nil
}

func readInlineCommand(r *bufio.Reader) ([][]byte, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}

	if strings.TrimSpace(line) == "" {
		return nil, ErrEmptyCommand
	}

	parts := strings.Fields(line)
	tokens := make([][]byte, 0, len(parts))
	for _, part := range parts {
		tokens = append(tokens, []byte(part))
	}

	return tokens, nil
}

func readLine(r *bufio.Reader) (string, error) {
	l, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}

	if len(l) < 2 || !strings.HasSuffix(l, "\r\n") {
		return "", fmt.Errorf("%w: invalid line terminator", ErrProtocol)
	}

	return strings.TrimSuffix(l, "\r\n"), nil
}
