package regress

import "testing"

func TestCompareDetectsP99Regression(t *testing.T) {
	report := Compare(
		Result{Label: "set", Totals: OperationStats{SuccessQPS: 100}, Latency: LatencyStats{P99MS: 10}},
		Result{Label: "set", Totals: OperationStats{SuccessQPS: 100}, Latency: LatencyStats{P99MS: 13}},
		Thresholds{MaxP99RegressionPercent: 20, MaxQPSRegressionPercent: 20},
	)
	if report.Status != "REGRESSION" {
		t.Fatalf("status = %s, want REGRESSION", report.Status)
	}
}

func TestCompareAcceptsWithinThreshold(t *testing.T) {
	report := Compare(
		Result{Label: "get", Totals: OperationStats{SuccessQPS: 100}, Latency: LatencyStats{P99MS: 10}},
		Result{Label: "get", Totals: OperationStats{SuccessQPS: 90}, Latency: LatencyStats{P99MS: 11}},
		Thresholds{MaxP99RegressionPercent: 20, MaxQPSRegressionPercent: 20},
	)
	if report.Status != "OK" {
		t.Fatalf("status = %s reason = %s, want OK", report.Status, report.Reason)
	}
}
