package resp

import (
	"bufio"
	"errors"
	"strings"
	"testing"
)

func TestReadCommandArray(t *testing.T) {
	t.Parallel()

	reader := bufio.NewReader(strings.NewReader("*1\r\n$4\r\nPING\r\n"))

	tokens, err := ReadCommand(reader)
	if err != nil {
		t.Fatalf("ReadCommand returned error: %v", err)
	}

	if len(tokens) != 1 {
		t.Fatalf("unexpected token count: %d", len(tokens))
	}
	if got := string(tokens[0]); got != "PING" {
		t.Fatalf("unexpected command token: %q", got)
	}
}

func TestReadCommandInline(t *testing.T) {
	t.Parallel()

	reader := bufio.NewReader(strings.NewReader("PING hello\r\n"))

	tokens, err := ReadCommand(reader)
	if err != nil {
		t.Fatalf("ReadCommand returned error: %v", err)
	}

	if len(tokens) != 2 {
		t.Fatalf("unexpected token count: %d", len(tokens))
	}
	if got := string(tokens[0]); got != "PING" {
		t.Fatalf("unexpected first token: %q", got)
	}
	if got := string(tokens[1]); got != "hello" {
		t.Fatalf("unexpected second token: %q", got)
	}
}

func TestReadCommandProtocolError(t *testing.T) {
	t.Parallel()

	reader := bufio.NewReader(strings.NewReader("*x\r\n"))

	_, err := ReadCommand(reader)
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("expected protocol error, got: %v", err)
	}
}
