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
func serverSetup() (*server.Membership, *server.LocalFiles, *net.UDPConn) {
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
	var s []*server.Request
	membership := &server.Membership{
		SrcHost: hostname,
		Data:    map[string]int64{hostname: time.Now().UnixNano() / int64(time.Millisecond)},
		List:    []string{hostname},
		Pending: s,
	}

	localFiles := &server.LocalFiles{
		Files: map[string][]string{},
		UpdateTimes: map[string]int64{},
	}

	server.ServerJoin(membership)
	return membership, localFiles, ser
}

func main() {
	hostname, _ := os.Hostname()
	membership, localFiles, ser := serverSetup()

	// Start a goroutine to handle sending out heartbeats
	go server.HeartbeatManager(membership)
	go server.ListenForUDP(ser, membership)
	go server.FileSystemManager(membership, localFiles)
	go server.TCPManager(membership, localFiles)

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
			// TODO: Need to fix. This is more complicated now
		case "leave":
			log.Infof("Node %s is leaving the network!", hostname)
			membership.Data[hostname] = 0
			time.Sleep(2 * time.Second)
			os.Exit(0)
		default:
			log.Infof("Command \"%s\" not recognized", input)
		}
	}
}
