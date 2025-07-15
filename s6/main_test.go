package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/minhtri06/1brc/writeresult"
)

func TestAggregate(t *testing.T) {
	inputFile := "../measurements_small.txt"
	expectedOutputFile := "../output_small.txt"

	agg, err := aggregate(inputFile)
	if err != nil {
		t.Fatalf("aggregate failed: %v", err)
	}

	// Convert agg to map[string]string for writeresult.ToFile
	res := make(map[string]string)
	for k, v := range agg.toMap() {
		res[k] = fmt.Sprintf("%.1f/%.1f/%.1f", v.Min, v.Mean, v.Max)
	}

	tmpDir := t.TempDir()
	tmpOutput := filepath.Join(tmpDir, "result.txt")
	err = writeresult.ToFile(tmpOutput, res)
	if err != nil {
		t.Fatalf("writeresult.ToFile failed: %v", err)
	}

	expected, err := os.ReadFile(expectedOutputFile)
	if err != nil {
		t.Fatalf("failed to read expected output: %v", err)
	}
	actual, err := os.ReadFile(tmpOutput)
	if err != nil {
		t.Fatalf("failed to read actual output: %v", err)
	}

	if string(expected) != string(actual) {
		t.Errorf("output mismatch\nExpected:\n%s\nActual:\n%s", expected, actual)
	}
}
