package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime/pprof"
)

const inputFile = "../measurements.txt"

// 1. Combine calculating value and separating name/value into one operation.
// 2. When get value from the map, instead of agg[name] (name is string), we use agg[string(name)] (name is []byte).
//    This makes a big improvement.

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
	Min  float64
	Mean float64
	Max  float64

	count  int32
	sumX10 int
}

func aggregate(inputFile string) (*customMap, error) {
	file, err := os.Open(inputFile)
	if err != nil {
		return nil, fmt.Errorf("cannot open file: %w", err)
	}
	defer file.Close()

	agg := NewCustomMap()
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
					Min:    val,
					Max:    val,
					sumX10: valX10,
					count:  1,
				},
			)
		} else {
			a.Max = max(a.Max, val)
			a.Min = min(a.Min, val)
			a.sumX10 += valX10
			a.count++
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan file %w", err)
	}

	agg.forEach(func(key []byte, a *Aggregation) {
		a.Mean = float64(a.sumX10) / float64(a.count) / 10
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
