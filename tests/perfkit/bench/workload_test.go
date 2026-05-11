package bench

import "testing"

func TestSequentialWorkload(t *testing.T) {
	workload, err := NewWorkload(Config{
		Distribution: Sequential,
		KeyPrefix:    "k:",
		Keyspace:     2,
		ValueSize:    3,
		WritePercent: 100,
	})
	if err != nil {
		t.Fatalf("new workload: %v", err)
	}

	first := workload.Next()
	second := workload.Next()
	third := workload.Next()
	if first.Key != "k:0" || second.Key != "k:1" || third.Key != "k:0" {
		t.Fatalf("sequential keys = %q, %q, %q", first.Key, second.Key, third.Key)
	}
	if string(first.Value) != "abc" {
		t.Fatalf("value = %q, want abc", first.Value)
	}
}

func TestWorkloadRejectsInvalidPercentages(t *testing.T) {
	_, err := NewWorkload(Config{
		ReadPercent:  50,
		WritePercent: 60,
	})
	if err == nil {
		t.Fatal("expected invalid percentage error")
	}
}
