package main

import (
	"bufio"
	"fmt"
	"io"
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
				Min:    val,
				Max:    val,
				sumX10: valX10,
				count:  1,
			}
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

	for _, a := range agg {
		a.Mean = float64(a.sumX10) / float64(a.count) / 10
	}

	return agg, nil
}

type Scanner struct {
	r              io.Reader
	buf            []byte
	pos            int
	end            int
	err            error
	station        []byte
	temperatureX10 int
	done           bool
}

func NewScanner(r io.Reader) *Scanner {
	return &Scanner{
		r:   r,
		buf: make([]byte, 0, 1024*1024), // 1MB buffer
	}
}

func (s *Scanner) Scan() bool {
	if s.done {
		return false
	}

	s.station = nil
	s.temperatureX10 = 0

	for ; s.pos < s.end; s.pos++ {
		if s.buf[s.pos] == ';' {
			s.station = s.buf[:s.pos]
			s.pos++
			break
		}
	}

	negative := false
	for ; s.pos < s.end; s.pos++ {
		if s.buf[s.pos] == '-' {
			negative = true
			continue
		}
		if s.buf[s.pos] == '.' {
			continue
		}
		if s.buf[s.pos] == '\n' {
			s.done = true
			s.pos++
			break
		}
		if s.buf[s.pos] == '\r' {
			s.pos++ 
			if s.pos < s.end && s.buf[s.pos] == '\n' {
			s.done = true
			break
		}
	}
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
