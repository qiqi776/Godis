package bench

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadSummary(t *testing.T) {
	root := t.TempDir()

	resultDir := filepath.Join(root, "cases", "alpha")
	if err := os.MkdirAll(resultDir, 0o755); err != nil {
		t.Fatalf("mkdir result dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(resultDir, "result.json"), []byte(`{
  "label": "alpha",
  "mode": "set",
  "routing": "leader",
  "concurrency": 16,
  "value_size": 64,
  "leader_endpoint": "127.0.0.1:6380",
  "totals": {"errors": 0, "success_qps": 123.45},
  "latency": {"p95_ms": 12.3, "p99_ms": 18.9}
}`), 0o644); err != nil {
		t.Fatalf("write result: %v", err)
	}

	faultDir := filepath.Join(root, "faults", "leader-kill")
	if err := os.MkdirAll(faultDir, 0o755); err != nil {
		t.Fatalf("mkdir fault dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(faultDir, "fault.json"), []byte(`{
  "scenario": "leader-kill",
  "profile": "steady",
  "target_node": "node1",
  "leader_before": "node1",
  "leader_after": "node2",
  "status": "ok"
}`), 0o644); err != nil {
		t.Fatalf("write fault: %v", err)
	}

	summary, err := LoadSummary(root)
	if err != nil {
		t.Fatalf("load summary: %v", err)
	}
	if len(summary.Results) != 1 {
		t.Fatalf("results len = %d, want 1", len(summary.Results))
	}
	if len(summary.Faults) != 1 {
		t.Fatalf("faults len = %d, want 1", len(summary.Faults))
	}

	markdown := summary.Markdown()
	if !strings.Contains(markdown, "| alpha | set | leader |") {
		t.Fatalf("markdown missing result row:\n%s", markdown)
	}
	if !strings.Contains(markdown, "| leader-kill | steady | node1 |") {
		t.Fatalf("markdown missing fault row:\n%s", markdown)
	}
}
