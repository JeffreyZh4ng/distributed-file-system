package main

import (
	"bufio"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"net"
	"os"
	"sort"
	"strings"
	"strconv"
	"sync"
	"time"
)

var PORT_NUM string = "4000"
var TIMEOUT_MS int64 = 3000
var INTRODUCER_NODE string = "fa19-cs425-g84-01.cs.illinois.edu"

// Need to store extra list that maintains order or the list
type Membership struct {
	SrcHost string
	Data    map[string]int64
	List    []string
}

// Writes with some error checking
func writeMembershipList(membership *Membership, hostName string) {
	conn, err := net.Dial("udp", hostName+":"+PORT_NUM)
	if err != nil {
		log.Fatal("Could not connect to node! %s", err)
	}
	defer conn.Close()

	memberSend, err := json.Marshal(membership)
	if err != nil {
		log.Fatal("Could not encode message %s", err)
		return
	}

	conn.Write(memberSend)
}

func sendHeartbeats(membership *Membership, wg *sync.WaitGroup) {
	hostName, _ := os.Hostname()
	index := findHostnameIndex(membership, hostName)
	lastIndex := index

	// Sends two heartbeats to its immediate successors
	for i := 1; i <= 2; i++ {
		nameIndex := (index + i) % len(membership.List)
		lastIndex = nameIndex
		if nameIndex == index {
			break
		}
		
		go writeMembershipList(membership, membership.List[nameIndex])
	}

	// Sends two heartbeats to its immediate predecessors
	for i := -1; i >= -2; i-- {
		nameIndex := (len(membership.List) + index + i) % len(membership.List)
		if lastIndex == index || nameIndex == lastIndex {
			break
		}
		
		go writeMembershipList(membership, membership.List[nameIndex])
	}

	wg.Done()
}

// Search that will find the index of the hostname in the list
func findHostnameIndex(membership *Membership, hostName string) (int) {
	for i := 0; i < len(membership.List); i++ {
		if membership.List[i] == hostName {
			return i
		}
	}

	return len(membership.List)
}

// Loops through the membership and checks if any of the times on the nodes are past the
// Allowed timeout. If the last time is 0 then the node has left the network
func removeExitedNodes(membership *Membership) {
	currTime := time.Now().UnixNano() / int64(time.Millisecond)
	tempList := membership.List[:0]
	rootName, _ := os.Hostname()

	for _, hostName := range membership.List {
		lastPing := membership.Data[hostName]
		if lastPing == 0 {
			if hostName != rootName {
				log.Infof("Node %s left the network!", hostName)
			} else {
				tempList = append(tempList, hostName)
			}
		} else if !(currTime-lastPing > TIMEOUT_MS) {
			tempList = append(tempList, hostName)
		} else {
			log.Infof("Node %s timed out!", hostName)
		}
	}

	membership.List = tempList
}

// Goroutine that will send out heartbeats every half second.
func heartbeatManager(membership *Membership) {
	hostname, _ := os.Hostname()
	ticker := time.NewTicker(500 * time.Millisecond)
	var wg sync.WaitGroup

	for {
		// This will block until the ticker sends a value to the chan
		<-ticker.C	
		wg.Add(1)
		go sendHeartbeats(membership, &wg)
		wg.Wait()		

		// If the current node has not left the network then update its time
		if membership.Data[hostname] != 0 {
			membership.Data[hostname] = time.Now().UnixNano() / int64(time.Millisecond)
		}
		removeExitedNodes(membership)
	}
}

// This function is called when the node recieves a UDP message
func processNewMembershipList(buffer []byte, readLen int, membership *Membership) {
	newMembership := Membership{}
	err := json.Unmarshal(buffer[:readLen], &newMembership)
	if err != nil {
		log.Infof("Could not decode request! %s", err)
		return
	}

	// This will loop through the membership recieved by the UDP request and update the
	// timestamps in the current node. 
	for i := 0; i < len(newMembership.List); i++ {
		nextHostname := newMembership.List[i]
		
		// If it finds a node that is not in its own list, add it.
		if pingTime, contains := membership.Data[nextHostname]; !contains {
			membership.Data[nextHostname] = newMembership.Data[nextHostname]
			
			membership.List = append(membership.List, nextHostname)
			sort.Strings(membership.List)	

		// If the current node just left the network or we get a UDP request that at node left
		} else if membership.Data[nextHostname] == 0 || newMembership.Data[nextHostname] == 0 {
			membership.Data[nextHostname] = 0

		// If the stored ping time is less than the UDP request time, update the time
		} else if pingTime < newMembership.Data[nextHostname] {	
			membership.Data[nextHostname] = newMembership.Data[nextHostname]
		
			// If the hostname is not in the list because it left or timedout and we recieved 
			// a newer time, add it back to the list
			currTime := time.Now().UnixNano() / int64(time.Millisecond)
			if findHostnameIndex(membership, nextHostname) >= len(membership.List) && 
			   currTime - newMembership.Data[nextHostname] < TIMEOUT_MS {
				
				membership.List = append(membership.List, nextHostname)
				sort.Strings(membership.List)	
				log.Infof("Recieved updated time from node %s. Adding back to list", nextHostname)	
			}	
		}
	}

	// Update the time of the node who sent the list. We need to check if the node has not left
	// because its possible that a goroutine might be finishing up even after the node left
	if membership.Data[newMembership.SrcHost] != 0 {
		membership.Data[newMembership.SrcHost] = time.Now().UnixNano() / int64(time.Millisecond)
	}
}

// Goroutine that constantly listens for incoming UDP calls
func readBuffer(ser *net.UDPConn, membership *Membership) {
	buffer := make([]byte, 1024)
	for {
		readLen, _, err := ser.ReadFromUDP(buffer)
		if err != nil {
			log.Infof("Encountered error while reading UDP socket! %s", err)
			continue
		}
		if readLen == 0 {
			continue
		}

		processNewMembershipList(buffer, readLen, membership)
	}
}

// Helper method that will connect to nodes based on if it is the introducer or not
func nodeSetup() (string, *Membership, *net.UDPConn) {
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
	membership := &Membership{
		SrcHost: hostname,
		Data:    map[string]int64{hostname: time.Now().UnixNano() / int64(time.Millisecond)},
		List:    []string{hostname},
	}

	// Add introducer code here. If the node is not the base node, send a heartbeat to the 0th node
	if hostname != INTRODUCER_NODE {
		log.Info("Writing to the introducer!")
		writeMembershipList(membership, INTRODUCER_NODE)
	} else {
		log.Info("Introducer attempting to reconnect to any node!")
		for i := 2; i <= 10; i++ {
			numStr := strconv.Itoa(i)
			if len(numStr) == 1 {
				numStr = "0" + numStr
			}

			connectName := "fa19-cs425-g84-" + numStr + ".cs.illinois.edu"
			writeMembershipList(membership, connectName)
		}
	}

	return hostname, membership, ser
}

func main() {
	hostname, membership, ser := nodeSetup()

	// Start a goroutine to handle sending out heartbeats
	go heartbeatManager(membership)
	go readBuffer(ser, membership)

	for {
		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		input = strings.TrimSuffix(input, "\n")

		switch input {
		case "id":
			log.Infof("Current node ID: %s", membership.SrcHost)
		case "list":
			log.Infof("Current list\n: %s", membership.List)
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
