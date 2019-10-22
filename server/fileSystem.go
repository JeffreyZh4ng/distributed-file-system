package server

import (
	log "github.com/sirupsen/logrus"
)

// Every heartbeat will also contain a list of all files in the system
type Directory struct {
	LastUpdate int64
	Storage    map[string][]string
}

func FileSystemManager(membership *Membership) {
	log.Info("TEST")
}
