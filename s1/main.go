package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
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
	Min    float64
	Mean   float64
	Max    float64
	SumX10 int64
	Count  int64
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

		valX10, err := strconv.Atoi(
			strings.Replace(valStr, ".", "", 1), // removing dot = value*10
		)
		if err != nil {
			return nil, fmt.Errorf("invalid value at line %v: %w", lineNum, err)
		}
		val := float64(valX10) / 10

		a, ok := agg[name]
		if !ok {
			a = &Aggregation{
				Min: val,
				Max: val,
			}
			agg[name] = a
		}

		if val > a.Max {
			a.Max = val
		}
		if val < a.Min {
			a.Min = val
		}
		a.SumX10 += int64(valX10)
		a.Count++
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan file %w", err)
	}

	for _, stationAgg := range agg {
		stationAgg.Mean = float64(stationAgg.SumX10) / float64(stationAgg.Count) / 10
	}

	return agg, nil
}
