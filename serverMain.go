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
	hostname, _ := os.Hostname()

	// Start a goroutine to handle sending out heartbeats
	go server.HeartbeatManager()
	go server.FileSystemManager()

	for {
		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		input = strings.TrimSuffix(input, "\n")

		switch input {
		case "id":
			log.Infof("Current node ID: %s", hostname)
		case "list":
			log.Infof("Current list:\n%s", server.Membership.List)
		case "store":
			fileList := []string{}
			for fileName, _ := range server.LocalFiles.Files {
				fileList = append(fileList, fileName)
			}
			log.Infof("Files stored in the server:\n%s", fileList)
		case "leave":
			log.Infof("Node %s is leaving the network!", hostname)
			server.Membership.Data[hostname] = -1 * server.Membership.Data[hostname]
			time.Sleep(2 * time.Second)
			os.Exit(0)
		default:
			log.Infof("Command \"%s\" not recognized", input)
		}
	}
}
