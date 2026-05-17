package compact

import (
	"testing"

	"mini-kv/internal/storage/lsm/sstable"
)

func TestPickerPickL0(t *testing.T) {
	state := &sstable.State{
		Levels: [][]sstable.TableMeta{{
			{FileNum: 1, Level: 0},
			{FileNum: 2, Level: 0},
		}},
	}
	job, ok := Picker{L0Trigger: 2}.Pick(state)
	if !ok {
		t.Fatal("Pick() = false, want true")
	}
	if job.Level != 0 || len(job.Inputs) != 2 {
		t.Fatalf("job = %+v, want level 0 with two inputs", job)
	}
}

func TestPickerSkipsBelowTrigger(t *testing.T) {
	state := &sstable.State{Levels: [][]sstable.TableMeta{{{FileNum: 1, Level: 0}}}}
	if _, ok := (Picker{L0Trigger: 2}).Pick(state); ok {
		t.Fatal("Pick() = true, want false")
	}
}