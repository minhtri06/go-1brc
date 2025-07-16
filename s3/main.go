package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"runtime/pprof"
)

// This solution is same as solution 2, but instead of using bytes.Cut, I separate name and value
// by iterating the line in reverse order to find the separator ';'.
// Take a look at separateNameValue function.

const inputFile = "../measurements.txt"

type Aggregation struct {
	min  float64
	mean float64
	max  float64

	sumX10 int64
	count  int64
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
		return nil, fmt.Errorf("cannot open file: %w", err)
	}
	defer file.Close()

	agg := map[string]*Aggregation{}
	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()

		bName, bVal, found := separateNameValue(line)
		if !found {
			return nil, fmt.Errorf("could not find name-value separator at line %v: %q", lineNum, line)
		}

		name := string(bName)

		valX10, err := evaluateValX10(bVal)
		if err != nil {
			return nil, fmt.Errorf("invalid value at line %v: %w", lineNum, err)
		}
		val := float64(valX10) / 10

		a, ok := agg[name]
		if !ok {
			agg[name] = &Aggregation{
				min:    val,
				mean:   val,
				max:    val,
				sumX10: int64(valX10),
				count:  1,
			}
		} else {
			a.min = min(a.min, val)
			a.max = max(a.max, val)
			a.sumX10 += int64(valX10)
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

func separateNameValue(line []byte) (name, value []byte, found bool) {
	for i := len(line) - 1; i >= 0; i-- {
		if line[i] == ';' {
			return line[:i], line[i+1:], true
		}
	}
	return nil, nil, false
}

// evaluateValX10 evaluates the value in bytes and return value multiply by 10 to avoid floating point precision issues.
// E.g. "12.3" will return 123, "-4.5" will return -45.
func evaluateValX10(valBytes []byte) (int64, error) {
	if len(valBytes) < 3 {
		return 0, errors.New("value too short")
	}

	val := int64(0)
	seenDot := false
	negative := false

	if valBytes[0] == '-' {
		negative = true
		valBytes = valBytes[1:]
	}

	for i, b := range valBytes {
		if b == '.' {
			if i != len(valBytes)-2 {
				return 0, errors.New("invalid dot position")
			}
			seenDot = true
			continue
		}
		if b < '0' || b > '9' {
			return 0, errors.New("invalid digit " + string(b))
		}
		val = val*10 + int64(b-'0')
	}

	if !seenDot {
		return 0, errors.New("missing dot in value")
	}
	if negative {
		val = -val
	}

	return val, nil
}
