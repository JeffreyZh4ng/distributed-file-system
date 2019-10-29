package server

import (
	"os"
	"time"
)

var FILESYSTEM_TIMEOUT_MS int64 = 6000

// Every heartbeat will also contain a list of all files in the system
type Requests struct {
	LastUpdate int64
	List	   map[string][]string
}

type fileSystem struct {
	
}

func FileSystemManager(membership *Membership) {
	hostname, _ := os.Hostname()
	ticker := time.NewTicker(1000 * time.Millisecond)

	for {
		<-ticker.C

		// The leader will be the lowest ID node
		if membership.List[0] != hostname {
			continue
		}

		// Loop through all nodes in the store map. If any of the nodes in the map have timeouts greater than the allowed timeout in the membership map then we need to remove them and figure out where to store the other files.
		currTime := time.Now().UnixNano() / int64(time.Millisecond)
		for serverName, _ := range membership.Pending.List {

			lastPing := membership.Data[serverName]
			if lastPing == 0 || currTime-lastPing > FILESYSTEM_TIMEOUT_MS {
				// Remove the node from the store and send the file
			}
		}
	}
}

func rebalanceFileSystem() {

}
