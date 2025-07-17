package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime/pprof"
)

// In this solution, we remove the bufio.Scanner and native map and combine the logic of them
// into a big single loop.
//
// With bufio.Scanner + native map, we have to traverse the line twice:
// 1. First is to find the newline character and get the line.
// 2. Second, we traverse the line again to process the name and value.
//
// And with this solution, we only traverse the line once.
// Also, it assumes the input file to be valid and contains less than 10,000 distinct station names.

const (
	inputFile = "../measurements.txt"
	bufSize   = 1024 * 1024 // 1 MB buffer size
	mapSize   = 131072      // 2^17, power of two
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

func aggregate(inputFile string) ([]*Entry, error) {
	file, err := os.Open(inputFile)
	if err != nil {
		return nil, fmt.Errorf("cannot open file: %w", err)
	}
	defer file.Close()

	// Custom map
	m := make([]*Entry, mapSize)
	// Custom Scanner to read the file
	buf := make([]byte, bufSize)
	readStart := 0
	done := false

	for !done {
		n, err := file.Read(buf[readStart:])
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
					// todo: add size of the map
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
		e.value.Mean = float64(e.value.sumX10) / float64(e.value.count) / 10
		agg = append(agg, e)
	}

	return agg, nil
}
