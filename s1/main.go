package main

import (
	"fmt"
	"time"
)

const inputFile = "../measurements.txt"

func main() {
	start := time.Now()

	_, err := aggregate(inputFile)
	if err != nil {
		panic(err)
	}

	elapsed := time.Since(start)
	fmt.Println("aggregate() took:", elapsed)
}
