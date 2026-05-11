package bench

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type LoadedResult struct {
	Path string `json:"path"`
	Result
}

type FaultReport struct {
	Path                 string `json:"path,omitempty"`
	Scenario             string `json:"scenario"`
	Profile              string `json:"profile"`
	TargetNode           string `json:"target_node"`
	LeaderBefore         string `json:"leader_before,omitempty"`
	LeaderAfter          string `json:"leader_after,omitempty"`
	Status               string `json:"status"`
	InjectDelay          string `json:"inject_delay,omitempty"`
	Outage               string `json:"outage,omitempty"`
	LeaderCommitIndex    uint64 `json:"leader_commit_index,omitempty"`
	FollowerAppliedIndex uint64 `json:"follower_applied_index,omitempty"`
	SnapshotCreateCount  uint64 `json:"snapshot_create_count,omitempty"`
	InstallSnapshotCount uint64 `json:"install_snapshot_count,omitempty"`
	ResultPath           string `json:"result_path,omitempty"`
	Note                 string `json:"note,omitempty"`
}

type Summary struct {
	Root    string         `json:"root"`
	Results []LoadedResult `json:"results"`
	Faults  []FaultReport  `json:"faults,omitempty"`
}

func LoadSummary(root string) (Summary, error) {
	summary := Summary{Root: root}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		switch entry.Name() {
		case "result.json":
			loaded, err := loadResult(path)
			if err != nil {
				return err
			}
			summary.Results = append(summary.Results, loaded)
		case "fault.json":
			fault, err := loadFault(path)
			if err != nil {
				return err
			}
			summary.Faults = append(summary.Faults, fault)
		}
		return nil
	})
	if err != nil {
		return Summary{}, err
	}

	sort.Slice(summary.Results, func(i, j int) bool {
		left := resultSortKey(summary.Results[i])
		right := resultSortKey(summary.Results[j])
		return left < right
	})
	sort.Slice(summary.Faults, func(i, j int) bool {
		if summary.Faults[i].Scenario != summary.Faults[j].Scenario {
			return summary.Faults[i].Scenario < summary.Faults[j].Scenario
		}
		return summary.Faults[i].Path < summary.Faults[j].Path
	})

	return summary, nil
}

func (s Summary) Markdown() string {
	var builder strings.Builder
	builder.WriteString("# Bench Summary\n\n")
	if len(s.Results) == 0 {
		builder.WriteString("No result.json files found.\n")
		return builder.String()
	}

	builder.WriteString("| Label | Mode | Routing | Concurrency | Value Bytes | Success QPS | Errors | P95 ms | P99 ms | Leader | Result |\n")
	builder.WriteString("|---|---|---|---:|---:|---:|---:|---:|---:|---|---|\n")
	for _, loaded := range s.Results {
		label := loaded.Label
		if label == "" {
			label = filepath.Base(filepath.Dir(loaded.Path))
		}
		fmt.Fprintf(&builder, "| %s | %s | %s | %d | %d | %.2f | %d | %.3f | %.3f | %s | %s |\n",
			escapePipe(label),
			escapePipe(loaded.Mode),
			escapePipe(loaded.Routing),
			loaded.Concurrency,
			loaded.ValueSize,
			loaded.Totals.SuccessQPS,
			loaded.Totals.Errors,
			loaded.Latency.P95MS,
			loaded.Latency.P99MS,
			escapePipe(loaded.LeaderEndpoint),
			escapePipe(relPathOrSelf(s.Root, loaded.Path)),
		)
	}

	if len(s.Faults) == 0 {
		return builder.String()
	}

	builder.WriteString("\n## Fault Scenarios\n\n")
	builder.WriteString("| Scenario | Profile | Target | Leader Before | Leader After | Status | Commit | Applied | Snapshot Create | Install Snapshot | Fault |\n")
	builder.WriteString("|---|---|---|---|---|---|---:|---:|---:|---:|---|\n")
	for _, fault := range s.Faults {
		fmt.Fprintf(&builder, "| %s | %s | %s | %s | %s | %s | %d | %d | %d | %d | %s |\n",
			escapePipe(fault.Scenario),
			escapePipe(fault.Profile),
			escapePipe(fault.TargetNode),
			escapePipe(fault.LeaderBefore),
			escapePipe(fault.LeaderAfter),
			escapePipe(fault.Status),
			fault.LeaderCommitIndex,
			fault.FollowerAppliedIndex,
			fault.SnapshotCreateCount,
			fault.InstallSnapshotCount,
			escapePipe(relPathOrSelf(s.Root, fault.Path)),
		)
	}

	return builder.String()
}

func loadResult(path string) (LoadedResult, error) {
	var result Result
	if err := decodeJSONFile(path, &result); err != nil {
		return LoadedResult{}, err
	}
	return LoadedResult{Path: path, Result: result}, nil
}

func loadFault(path string) (FaultReport, error) {
	var fault FaultReport
	if err := decodeJSONFile(path, &fault); err != nil {
		return FaultReport{}, err
	}
	fault.Path = path
	return fault, nil
}

func decodeJSONFile(path string, target any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

func resultSortKey(loaded LoadedResult) string {
	if loaded.Label != "" {
		return loaded.Label
	}
	return loaded.Path
}

func relPathOrSelf(root, path string) string {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return path
	}
	return rel
}

func escapePipe(input string) string {
	if input == "" {
		return ""
	}
	return strings.ReplaceAll(input, "|", "\\|")
}
