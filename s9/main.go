package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
)

const (
	inputFile = "../measurements.txt"

	// Custom Scanner
	scanBufSize = 1024 * 1024 // 1 MB buffer size

	// Custom map
	mapSize   = 131072 // 2^17, power of two
	mapMask   = mapSize - 1
	maxLoad   = mapSize / 2 // panic if more than half full, we know there's no more 10k station names
	fnvOffset = 14695981039346656037
	fnvPrime  = 1099511628211
)

type Aggregation struct {
	Min  float64
	Mean float64
	Max  float64

	count  int32
	sumX10 int
}

type Entry struct {
	name  []byte
	value *Aggregation
}

func main() {
	profFile, err := os.Create("cpu.prof")
	if err != nil {
		panic(fmt.Errorf("cannot create prof file: %w", err))
	}
	defer profFile.Close()

	if err := pprof.StartCPUProfile(profFile); err != nil {
		panic(fmt.Errorf("cannot start CPU profile: %w", err))
	}
	defer pprof.StopCPUProfile()

	_, err = Aggregate(inputFile)
	if err != nil {
		panic(fmt.Errorf("aggregate failed: %w", err))
	}
}

func Aggregate(filename string) (map[string]*Aggregation, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("cannot open file: %w", err)
	}
	defer f.Close()

	numWorkers := runtime.NumCPU() // Number of readers, set as the number of CPU cores
	if numWorkers < 1 {
		numWorkers = 16
	}

	FileParts, err := splitsFile(inputFile, numWorkers)
	if err != nil {
		return nil, fmt.Errorf("cannot spit file: %w", err)
	}

	type result struct {
		entries []*Entry
		err     error
	}
	resCh := make(chan *result, numWorkers+1)

	go func() {
		defer close(resCh)

		var wg sync.WaitGroup

		for _, part := range FileParts {
			r := io.NewSectionReader(f, part.offset, part.length)

			wg.Add(1)
			go func(r io.Reader) {
				defer wg.Done()

				e, err := aggregate(r)
				if err != nil {
					resCh <- &result{nil, err}
					return
				}
				resCh <- &result{entries: e, err: nil}
			}(r)
		}

		wg.Wait()
	}()

	agg := make(map[string]*Aggregation, 10000)
	for res := range resCh {
		if res.err != nil {
			return nil, res.err
		}
		if res.entries == nil {
			continue
		}
		for _, entry := range res.entries {
			a, ok := agg[string(entry.name)]
			if !ok {
				agg[string(entry.name)] = entry.value
				continue
			}
			a.Max = max(a.Max, entry.value.Max)
			a.Min = min(a.Min, entry.value.Min)
			a.sumX10 += entry.value.sumX10
			a.count += entry.value.count
		}
	}

	for _, a := range agg {
		a.Mean = float64(a.sumX10) / float64(a.count) / 10
	}

	return agg, nil
}

func aggregate(r io.Reader) ([]*Entry, error) {
	// Custom map
	m := make([]*Entry, mapSize)
	size := 0
	// Custom Scanner to read the file
	buf := make([]byte, scanBufSize)
	readStart := 0
	done := false

	for !done {
		n, err := r.Read(buf[readStart:])
		if err != nil {
			if err != io.EOF {
				return nil, fmt.Errorf("failed to read file: %w", err)
			}
			done = true
		}
		length := readStart + n

		// According the the spec, we know the length of each line is no longer than 1 MB.
		chunk := buf[:bytes.LastIndexByte(buf[:length], '\n')+1] // Include the newline character

		for i := 0; i < len(chunk); {
			// Find the station name and calculate the hash on the way
			hash := uint64(fnvOffset)
			start := i
			for ; chunk[i] != ';'; i++ {
				hash ^= uint64(chunk[i])
				hash *= uint64(fnvPrime)
			}
			name := chunk[start:i]
			i++ // Skip the newline character

			// Calculate the value
			valX10 := 0
			negative := false
			for ; chunk[i] != '\n'; i++ {
				if chunk[i] == '-' {
					negative = true
					continue
				}
				if chunk[i] == '.' {
					continue
				}
				valX10 = valX10*10 + int(chunk[i]-'0')
			}
			i++ // Skip the newline character
			if negative {
				valX10 = -valX10
			}
			val := float64(valX10) / 10

			// Set value into the map
			bucket := hash & mapMask
			for ; ; bucket++ {
				e := m[bucket]
				if e == nil {
					// Empty slot, insert here
					m[bucket] = &Entry{
						name:  make([]byte, len(name)),
						value: &Aggregation{Min: val, Max: val, sumX10: valX10, count: 1},
					}
					copy(m[bucket].name, name)
					size++
					if size >= maxLoad {
						panic("custom map exceeded maximum load factor")
					}
					break
				}
				if bytes.Equal(e.name, name) {
					// Key already exists, update value
					e.value.Max = max(e.value.Max, val)
					e.value.Min = min(e.value.Min, val)
					e.value.sumX10 += valX10
					e.value.count++
					break
				}
			}
		}

		copy(buf, buf[len(chunk):length])
		readStart = length - len(chunk)
	}

	agg := make([]*Entry, 0, len(m))
	for _, e := range m {
		if e == nil {
			continue
		}
		agg = append(agg, e)
	}

	return agg, nil
}
