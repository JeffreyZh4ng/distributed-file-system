package server

import (
	log "github.com/sirupsen/logrus"
	"os"
	"time"
)

// Every heartbeat will also contain a list of all files in the system
type Directory struct {
	LastUpdate int64
	Storage    map[string][]string
}

func FileSystemManager(membership *Membership) {
	hostname, _ := os.Hostname()
	ticker := time.NewTicker(1000 * time.Millisecond)

	for {
		<-ticker.C
		if membership.List[0] != hostname {
			continue
		}

		// Loop through all nodes in the store map. If any of the nodes in the map have timeouts greater than the allowed timeout in the membership map then we need to remove them and figure out where to stoer the other files.
		currTime := time.Now().UnixNano() / int64(time.Millisecond)
		for serverName, _ := range membership.Store.Storage {
			lastPing := membership.Data[hostname]
			if lastPing == 0 || currTime-lastPing > TIMEOUT_MS {
				// Node has timed out. Remove it from the map
				log.Infof("%s", serverName)
			}
		}
	}
}

func rebalanceFileSystem() {
}
