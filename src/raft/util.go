package raft

import "log"
import "os"

// Debugging
//const Debug = 0

/* print debug message according to environment variable */
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if os.Getenv("GO_DEBUG") == "1" {
		log.Printf(format, a...)
	}
	return
}

func MaxInt(x, y int) int {
	if x < y {
		return y
	}
	return x
}

func MinInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}
