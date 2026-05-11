package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"mini-kv/internal/bench"
)

func main() {
	var inputDir string
	var markdownPath string
	var jsonPath string

	flag.StringVar(&inputDir, "input-dir", "", "result root directory")
	flag.StringVar(&markdownPath, "markdown", "", "optional markdown output path")
	flag.StringVar(&jsonPath, "json", "", "optional JSON output path")
	flag.Parse()

	if inputDir == "" {
		fmt.Fprintln(os.Stderr, "input-dir is required")
		os.Exit(2)
	}

	summary, err := bench.LoadSummary(inputDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load summary: %v\n", err)
		os.Exit(1)
	}

	markdown := summary.Markdown()
	fmt.Print(markdown)

	if markdownPath != "" {
		if err := writeFile(markdownPath, []byte(markdown)); err != nil {
			fmt.Fprintf(os.Stderr, "write markdown: %v\n", err)
			os.Exit(1)
		}
	}
	if jsonPath != "" {
		data, err := json.MarshalIndent(summary, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "marshal json: %v\n", err)
			os.Exit(1)
		}
		if err := writeFile(jsonPath, data); err != nil {
			fmt.Fprintf(os.Stderr, "write json: %v\n", err)
			os.Exit(1)
		}
	}
}

func writeFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}
