package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime/pprof"
)

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
	Min  float64
	Mean float64
	Max  float64

	MinX10 int
	MaxX10 int
	SumX10 int64
	Count  int64
}

func aggregate(inputFile string) (map[string]*Aggregation, error) {
	file, err := os.Open(inputFile)
	if err != nil {
		return nil, fmt.Errorf("cannot open file: %w", err)
	}
	defer file.Close()

	agg := map[string]*Aggregation{}
	scanner := bufio.NewScanner(file)
	lineNum := 0
	sep := []byte(";")

	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()

		bName, bVal, found := bytes.Cut(line, sep)
		if !found {
			return nil, fmt.Errorf("could not find name-value separator at line %v: %q", lineNum, line)
		}

		name := string(bName)

		valX10, err := evaluateValX10(bVal)
		if err != nil {
			return nil, fmt.Errorf("invalid value at line %v: %w", lineNum, err)
		}

		a, ok := agg[name]
		if !ok {
			a = &Aggregation{
				MinX10: math.MaxInt,
				MaxX10: math.MinInt,
			}
			agg[name] = a
		}

		if valX10 > a.MaxX10 {
			a.MaxX10 = valX10
		}
		if valX10 < a.MinX10 {
			a.MinX10 = valX10
		}
		a.SumX10 += int64(valX10)
		a.Count++
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan file %w", err)
	}

	for _, stationAgg := range agg {
		stationAgg.Mean = float64(stationAgg.SumX10) / float64(stationAgg.Count) / 10
		stationAgg.Min = float64(stationAgg.MinX10) / 10
		stationAgg.Max = float64(stationAgg.MaxX10) / 10
	}

	return agg, nil
}

// evaluateValX10 evaluates the value in bytes and return value multiply by 10 to avoid floating point precision issues.
// E.g. "12.3" will return 123, "-4.5" will return -45.
func evaluateValX10(valBytes []byte) (int, error) {
	val := 0
	seenDot := false
	negative := false

	for _, b := range valBytes {
		if b == '.' {
			seenDot = true
			continue
		}
		if b == '-' {
			negative = true
			continue
		}
		if b < '0' || b > '9' {
			return 0, errors.New("invalid digit " + string(b))
		}
		val = val*10 + int(b-'0')
		if seenDot {
			break
		}
	}

	if negative {
		val = -val
	}
	return val, nil
}
