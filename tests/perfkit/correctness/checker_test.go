package correctness

import (
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

func TestCheckAcceptsLinearizableHistory(t *testing.T) {
	base := time.Unix(0, 0)
	report := Check([]Operation{
		{
			ClientID: 1,
			Type:     OpSet,
			Key:      "a",
			Value:    []byte("1"),
			Call:     base,
			Return:   base.Add(time.Millisecond),
		},
		{
			ClientID: 2,
			Type:     OpGet,
			Key:      "a",
			Call:     base.Add(2 * time.Millisecond),
			Return:   base.Add(3 * time.Millisecond),
			Output:   Output{Value: []byte("1"), Found: true},
		},
	}, time.Second)
	if report.Result != porcupine.Ok {
		t.Fatalf("result = %s, want %s", report.Result, porcupine.Ok)
	}
}

func TestCheckRejectsIllegalHistory(t *testing.T) {
	base := time.Unix(0, 0)
	report := Check([]Operation{
		{
			ClientID: 1,
			Type:     OpSet,
			Key:      "a",
			Value:    []byte("1"),
			Call:     base,
			Return:   base.Add(time.Millisecond),
		},
		{
			ClientID: 2,
			Type:     OpGet,
			Key:      "a",
			Call:     base.Add(2 * time.Millisecond),
			Return:   base.Add(3 * time.Millisecond),
			Output:   Output{Value: []byte("2"), Found: true},
		},
	}, time.Second)
	if report.Result != porcupine.Illegal {
		t.Fatalf("result = %s, want %s", report.Result, porcupine.Illegal)
	}
}

func TestCheckSkipsFailedOperations(t *testing.T) {
	base := time.Unix(0, 0)
	report := Check([]Operation{
		{
			ClientID: 1,
			Type:     OpSet,
			Key:      "a",
			Value:    []byte("1"),
			Call:     base,
			Return:   base.Add(time.Millisecond),
			Err:      "not leader",
		},
	}, time.Second)
	if report.Result != porcupine.Ok || report.Checked != 0 || report.SkippedErr != 1 {
		t.Fatalf("report = %+v", report)
	}
}
