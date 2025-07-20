package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
)

type FilePart struct {
	offset int64
	length int64
}

func splitsFile(filename string, numParts int) ([]FilePart, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("cannot open file: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get stat file: %w", err)
	}

	const bufSize = 32

	size := stat.Size()
	partSize := size / int64(numParts)
	parts := []FilePart{}
	offset := int64(0)
	buf := make([]byte, bufSize)

	for i := range numParts {
		if i == numParts-1 {
			// If it's the last part, take all the rest
			if offset < size {
				parts = append(parts, FilePart{offset, size - offset})
			}
			break
		}

		end := offset + partSize - bufSize
		if _, err := file.Seek(end, io.SeekStart); err != nil {
			return nil, fmt.Errorf("failed to seek file: %w", err)
		}

		// Move end to the next newline character
		for {
			n, err := file.Read(buf)
			if err != nil && err != io.EOF {
				return nil, err
			}
			if n == 0 {
				break
			}
			newlineIdx := bytes.IndexByte(buf[:n], '\n')
			if newlineIdx != -1 {
				end += int64(newlineIdx) + 1
				break
			}
			end += int64(n)
		}

		parts = append(parts, FilePart{offset, end - offset})
		offset = end
	}

	return parts, nil
}
