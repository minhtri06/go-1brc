package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime/pprof"
)

// This solution uses a custom map implementation to aggregate measurements.
// It assumes the input file to be valid and contains less than 10,000 distinct station names.

const inputFile = "../measurements.txt"

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

type Aggregation struct {
	min  float64
	mean float64
	max  float64

	count  int32
	sumX10 int
}

func aggregate(inputFile string) (*customMap, error) {
	file, err := os.Open(inputFile)
	if err != nil {
		return nil, fmt.Errorf("cannot open file: %w", err)
	}
	defer file.Close()

	agg := newCustomMap()
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Bytes()

		name, valX10 := extractNameValueX10(line)
		val := float64(valX10) / 10

		a, exists := agg.get(name)
		if !exists {
			agg.set(
				name,
				&Aggregation{
					min:    val,
					max:    val,
					sumX10: valX10,
					count:  1,
				},
			)
		} else {
			a.max = max(a.max, val)
			a.min = min(a.min, val)
			a.sumX10 += valX10
			a.count++
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan file %w", err)
	}

	agg.forEach(func(k []byte, a *Aggregation) {
		a.mean = float64(a.sumX10) / float64(a.count) / 10
	})

	return agg, nil
}

func extractNameValueX10(line []byte) (name []byte, valX10 int) {
	factor := 1
	for i := len(line) - 1; i >= 0; i-- {
		if line[i] == '.' {
			continue
		}
		if line[i] == '-' {
			valX10 = -valX10
			continue
		}
		if line[i] == ';' {
			return line[:i], valX10
		}
		valX10 = int(line[i]-'0')*factor + valX10
		factor *= 10
	}
	return nil, 0
}
