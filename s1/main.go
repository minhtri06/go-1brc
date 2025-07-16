package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
)

// This solution is idiomatic and simple.
// It doesn't assume the file format to be valid, if the file is malformed, just return an error.

const inputFile = "../measurements.txt"

type Aggregation struct {
	min  float64
	mean float64
	max  float64

	sum   float64
	count int
}

func main() {
	f, err := os.Create("cpu.prof")
	if err != nil {
		panic(fmt.Errorf("cannot create prof file: %w", err))
	}
	defer f.Close()

	if err := pprof.StartCPUProfile(f); err != nil {
		panic(fmt.Errorf("cannot start CPU profile: %w", err))
	}
	defer pprof.StopCPUProfile()

	_, err = aggregate(inputFile)
	if err != nil {
		panic(fmt.Errorf("aggregate failed: %w", err))
	}
}

func aggregate(inputFile string) (map[string]*Aggregation, error) {
	file, err := os.Open(inputFile)
	if err != nil {
		return nil, fmt.Errorf("cannot open file %q. caused by: %w", inputFile, err)
	}
	defer file.Close()

	agg := map[string]*Aggregation{}
	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		sepIdx := strings.Index(line, ";")
		if sepIdx == -1 {
			return nil, fmt.Errorf("invalid format at line %v: %q", lineNum, line)
		}
		name, valStr := line[:sepIdx], line[sepIdx+1:]

		val, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid value at line %v: %w", lineNum, err)
		}

		a, ok := agg[name]
		if !ok {
			agg[name] = &Aggregation{
				min:   val,
				max:   val,
				sum:   val,
				count: 1,
			}
		} else {
			a.min = min(a.min, val)
			a.max = max(a.max, val)
			a.sum += val
			a.count++
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan file %w", err)
	}

	for _, a := range agg {
		a.mean = a.sum / float64(a.count)
	}

	return agg, nil
}
