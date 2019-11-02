package server

import (
	"os"
	"time"
)

var FILESYSTEM_TIMEOUT_MS int64 = 6000

// Every heartbeat will also contain a list of all files in the system
type Request struct {
	Type     string
	SrcHost  string
	FileName string
}

// Local datastore that keeps track of the other nodes that have the same files
type localFiles struct {
	fileLocations  map[string][]string
	fileUpdateTime map[string]int64
}

func FileSystemManager(membership *Membership) {
	hostname, _ := os.Hostname()
	ticker := time.NewTicker(1000 * time.Millisecond)
	
	for {
		<-ticker.C
		// Check if there are any requests in the membership list
		for i := 0; i < len(membership.Pending); i++ {
			// Do different things based on each request
		}
	}
}

