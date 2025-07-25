package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime/pprof"
)

// This solution adds two improvements to solution 4:
// 1. Combine calculating value and separating name/value into one operation -> Reduce bytes traversal.
// 2. Instead of define an intermediate variable for map keys `name := string(bName)`, we directly use
//    `string(name)` when accessing the map -> Reduce the overhead of creating an intermediate variable.
//
// This solution assumes the input file to be valid.

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

func aggregate(inputFile string) (map[string]*Aggregation, error) {
	file, err := os.Open(inputFile)
	if err != nil {
		return nil, fmt.Errorf("cannot open file: %w", err)
	}
	defer file.Close()

	agg := make(map[string]*Aggregation, 1000)
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Bytes()

		name, valX10 := extractNameValueX10(line)
		val := float64(valX10) / 10

		a, ok := agg[string(name)]
		if !ok {
			agg[string(name)] = &Aggregation{
				min:    val,
				max:    val,
				sumX10: valX10,
				count:  1,
			}
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

	for _, a := range agg {
		a.mean = float64(a.sumX10) / float64(a.count) / 10
	}

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
