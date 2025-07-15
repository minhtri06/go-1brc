package main

import "fmt"

func main() {
	pos := 0

	for ; pos < 10; pos++ {
		if pos == 5 {
			break
		}
	}

	fmt.Println(pos)
}
