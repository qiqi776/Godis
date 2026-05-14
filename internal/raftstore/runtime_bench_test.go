package raftstore

import (
	"testing"

	"mini-kv/internal/kv"
)

var benchmarkCommandSink kv.Command
var benchmarkBytesSink []byte

func benchmarkCommand() kv.Command {
	return kv.Command{
		Type:      kv.CommandPut,
		Key:       "bench:key:0000000000000001",
		Value:     []byte("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
		ClientID:  "bench-client-0001",
		RequestID: 42,
	}
}

func BenchmarkEncodeCommand(b *testing.B) {
	command := benchmarkCommand()
	b.ReportAllocs()

	for b.Loop() {
		out, err := EncodeCommand(command)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkBytesSink = out
	}
}

func BenchmarkDecodeCommand(b *testing.B) {
	data, err := EncodeCommand(benchmarkCommand())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()

	for b.Loop() {
		out, err := DecodeCommand(data)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkCommandSink = out
	}
}
