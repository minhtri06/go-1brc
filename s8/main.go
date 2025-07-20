package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
)

// This solution add parallelism to the solution 2, which I think is the balance between efficient and simple.
// It separates the file into multiple chunks and each read them concurrently.

type Aggregation struct {
	min  float64
	mean float64
	max  float64

	sumX10 int64
	count  int64
}

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

	_, err = Aggregate(inputFile)
	if err != nil {
		panic(err)
	}
}

func Aggregate(inputFile string) (map[string]*Aggregation, error) {
	file, err := os.Open(inputFile)
	if err != nil {
		return nil, fmt.Errorf("cannot open file: %w", err)
	}
	defer file.Close()

	numWorkers := runtime.NumCPU()
	if numWorkers < 1 {
		numWorkers = 16
	}

	fileParts, err := splitsFile(inputFile, numWorkers)
	if err != nil {
		return nil, fmt.Errorf("cannot split file: %w", err)
	}

	type result struct {
		agg map[string]*Aggregation
		err error
	}
	resCh := make(chan *result, len(fileParts))

	go func() {
		defer close(resCh)

		var wg sync.WaitGroup

		for _, part := range fileParts {
			r := io.NewSectionReader(file, part.offset, part.length)

			wg.Add(1)
			go func(r io.Reader) {
				defer wg.Done()

				agg, err := aggregate(r)
				if err != nil {
					resCh <- &result{nil, fmt.Errorf("failed to aggregate: %w", err)}
					return
				}
				resCh <- &result{agg, nil}
			}(r)
		}

		wg.Wait()
	}()

	agg := make(map[string]*Aggregation, 10000)
	for res := range resCh {
		if res.err != nil {
			return nil, res.err
		}
		for k, v := range res.agg {
			a, ok := agg[k]
			if !ok {
				agg[k] = v
			} else {
				a.min = min(a.min, v.min)
				a.max = max(a.max, v.max)
				a.sumX10 += v.sumX10
				a.count += v.count
			}
		}
	}

	for _, a := range agg {
		a.mean = float64(a.sumX10) / float64(a.count) / 10
	}

	return agg, nil
}

func aggregate(r io.Reader) (map[string]*Aggregation, error) {
	agg := map[string]*Aggregation{}
	scanner := bufio.NewScanner(r)

	for scanner.Scan() {
		line := scanner.Bytes()

		newlineIdx := bytes.IndexByte(line, ';')
		if newlineIdx == -1 {
			return nil, fmt.Errorf("could not find separator in line")
		}

		bName, bVal := line[:newlineIdx], line[newlineIdx+1:]

		name := string(bName)

		valX10, err := evaluateValX10(bVal)
		if err != nil {
			return nil, fmt.Errorf("invalid value '%v': %w", string(bVal), err)
		}
		val := float64(valX10) / 10

		a, ok := agg[name]
		if !ok {
			agg[name] = &Aggregation{
				min:    val,
				mean:   val,
				max:    val,
				sumX10: valX10,
				count:  1,
			}
		} else {
			a.min = min(a.min, val)
			a.max = max(a.max, val)
			a.sumX10 += valX10
			a.count++
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan file %w", err)
	}

	return agg, nil
}

// evaluateValX10 evaluates the value in bytes and return value multiply by 10 to avoid floating point numbers.
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
