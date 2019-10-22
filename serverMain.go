package main

import (	
	log "github.com/sirupsen/logrus"
	"cs-425-mp3/server"
	"bufio"
	"net"
	"os"
	"strings"
	"time"
)

var PORT_NUM string = "4000"

// Helper method that will connect to nodes based on if it is the introducer or not
func serverSetup() (*server.Membership, *net.UDPConn) {
	hostname, _ := os.Hostname()
	addr, err := net.ResolveUDPAddr("udp", hostname+":"+PORT_NUM)
	if err != nil {
		log.Fatal("Could not resolve hostname!: %s", err)
	}
	ser, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Fatal("Server could not set up UDP listener %s", err)
	}
	log.Infof("Connected to %s!", hostname)

	// Initialize struct to include itself in its membership list
	fileSystem := &server.Directory{
		LastUpdate: time.Now().UnixNano() / int64(time.Millisecond),
		Storage: map[string][]string{},
	}

	membership := &server.Membership{
		SrcHost: hostname,
		Data:    map[string]int64{hostname: time.Now().UnixNano() / int64(time.Millisecond)},
		List:    []string{hostname},
		Store:   fileSystem,
	}

	server.ServerJoin(membership)
	return membership, ser
}

func main() {
	hostname, _ := os.Hostname()
	membership, ser := serverSetup()

	// Start a goroutine to handle sending out heartbeats
	go server.HeartbeatManager(membership)
	go server.ListenForUDP(ser, membership)
	go server.FileSystemManager(membership)

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
