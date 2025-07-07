package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"time"
)

const inputFile = "../measurements.txt"

// This solution is like s1, but instead of Scanning a line into text we scan it into bytes

func main() {
	start := time.Now()

	_, err := aggregate(inputFile)
	if err != nil {
		panic(err)
	}

	elapsed := time.Since(start)
	fmt.Println("aggregate() took:", elapsed)
}

type Aggregation struct {
	Min    float64
	Mean   float64
	Max    float64
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
				Min: math.MaxFloat64,
				Max: -math.MaxFloat64,
			}
			agg[name] = a
		}

		a.SumX10 += int64(valX10)
		a.Count++

		val := float64(valX10) / 10
		if val > a.Max {
			a.Max = val
		}
		if val < a.Min {
			a.Min = val
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan file %w", err)
	}

	for _, stationAgg := range agg {
		stationAgg.Mean = float64(stationAgg.SumX10) / float64(stationAgg.Count) / 10
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
