// Package randaddr provides helpers to generate network address with random port number.
package randaddr

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

var r = rand.New(rand.NewSource(time.Now().UnixNano()))

const (
	// Currently only these ports are whitelisted on AWS security group!
	PortStart = 30001
	PortEnd   = 35000
	PortRange = PortEnd - PortStart
)

// Returns a randome port number in range [PortStart, PortEnd)
func RandPort() int {
	return PortStart + int(r.Intn(PortRange))
}

// Resolves the port number of a network address string if it ends with ":rand".
// If the string does not end with ":rand", the string is returned unchanged
func Resolve(s string) string {
	if strings.HasSuffix(s, ":rand") {
		s = strings.TrimSuffix(s, ":rand")
		s = fmt.Sprintf("%s:%d", s, RandPort())
	}
	return s
}

// A shortcut for Resolve("localhost:rand")
func Local() string {
	return fmt.Sprintf("localhost:%d", RandPort())
}
