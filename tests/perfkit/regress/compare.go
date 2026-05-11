package regress

import (
	"encoding/json"
	"fmt"
	"os"
)

type Result struct {
	Label   string         `json:"label"`
	Totals  OperationStats `json:"totals"`
	Latency LatencyStats   `json:"latency"`
}

type OperationStats struct {
	SuccessQPS float64 `json:"success_qps"`
	Errors     uint64  `json:"errors"`
}

type LatencyStats struct {
	P99MS float64 `json:"p99_ms"`
}

type Thresholds struct {
	MaxP99RegressionPercent float64 `yaml:"max_p99_regression_percent"`
	MaxQPSRegressionPercent float64 `yaml:"max_qps_regression_percent"`
	MaxErrorIncrease        uint64  `yaml:"max_error_increase"`
}

type Comparison struct {
	Label                string  `json:"label"`
	Status               string  `json:"status"`
	BaselineQPS          float64 `json:"baseline_qps"`
	CurrentQPS           float64 `json:"current_qps"`
	QPSRegressionPercent float64 `json:"qps_regression_percent"`
	BaselineP99MS        float64 `json:"baseline_p99_ms"`
	CurrentP99MS         float64 `json:"current_p99_ms"`
	P99RegressionPercent float64 `json:"p99_regression_percent"`
	BaselineErrors       uint64  `json:"baseline_errors"`
	CurrentErrors        uint64  `json:"current_errors"`
	Reason               string  `json:"reason,omitempty"`
}

func LoadResult(path string) (Result, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Result{}, err
	}
	var result Result
	if err := json.Unmarshal(data, &result); err != nil {
		return Result{}, err
	}
	return result, nil
}

func Compare(baseline Result, current Result, thresholds Thresholds) Comparison {
	comparison := Comparison{
		Label:          current.Label,
		Status:         "OK",
		BaselineQPS:    baseline.Totals.SuccessQPS,
		CurrentQPS:     current.Totals.SuccessQPS,
		BaselineP99MS:  baseline.Latency.P99MS,
		CurrentP99MS:   current.Latency.P99MS,
		BaselineErrors: baseline.Totals.Errors,
		CurrentErrors:  current.Totals.Errors,
	}
	if comparison.Label == "" {
		comparison.Label = baseline.Label
	}
	if baseline.Totals.SuccessQPS > 0 && current.Totals.SuccessQPS < baseline.Totals.SuccessQPS {
		comparison.QPSRegressionPercent = percent(baseline.Totals.SuccessQPS-current.Totals.SuccessQPS, baseline.Totals.SuccessQPS)
	}
	if baseline.Latency.P99MS > 0 && current.Latency.P99MS > baseline.Latency.P99MS {
		comparison.P99RegressionPercent = percent(current.Latency.P99MS-baseline.Latency.P99MS, baseline.Latency.P99MS)
	}

	if comparison.QPSRegressionPercent > thresholds.MaxQPSRegressionPercent {
		return comparison.regression(fmt.Sprintf("qps regression %.2f%% > %.2f%%", comparison.QPSRegressionPercent, thresholds.MaxQPSRegressionPercent))
	}
	if comparison.P99RegressionPercent > thresholds.MaxP99RegressionPercent {
		return comparison.regression(fmt.Sprintf("p99 regression %.2f%% > %.2f%%", comparison.P99RegressionPercent, thresholds.MaxP99RegressionPercent))
	}
	if current.Totals.Errors > baseline.Totals.Errors+thresholds.MaxErrorIncrease {
		return comparison.regression(fmt.Sprintf("errors increased from %d to %d", baseline.Totals.Errors, current.Totals.Errors))
	}
	return comparison
}

func (c Comparison) regression(reason string) Comparison {
	c.Status = "REGRESSION"
	c.Reason = reason
	return c
}

func percent(delta float64, base float64) float64 {
	if base == 0 {
		return 0
	}
	return delta / base * 100
}
