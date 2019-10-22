package main

import (	
	log "github.com/sirupsen/logrus"
	"cs-425-mp3/server"
	"bufio"
	"os"
	"strings"
	"time"
)

func main() {
	hostname, membership, ser := server.NodeSetup()

	// Start a goroutine to handle sending out heartbeats
	go server.HeartbeatManager(membership)
	go server.ListenForUDP(ser, membership)

	for {
		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		input = strings.TrimSuffix(input, "\n")

		switch input {
		case "id":
			log.Infof("Current node ID: %s", membership.SrcHost)
		case "list":
			log.Infof("Current list:\n%s", membership.List)
		case "store":
			if fileList, contains := membership.Store.Storage[hostname]; !contains {
				log.Info("No files stored at this node!")
			} else {
				log.Infof("Files at this node:\n%s", fileList)
			}
		case "leave":
			log.Infof("Node %s is leaving the network!", hostname)
			membership.Data[hostname] = 0
			time.Sleep(3 * time.Second)
			os.Exit(0)
		default:
			log.Infof("Command \"%s\" not recognized", input)
		}
	}
}
