package server

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

var PORT_NUM string = "4000"
var TIMEOUT_MS int64 = 3000
var INTRODUCER_NODE string = "fa19-cs425-g84-01.cs.illinois.edu"

// Need to store extra list that maintains order or the list
// MP3 membership also handles pending requests
type Membership struct {
	SrcHost string
	Data    map[string]int64
	List    []string

	LeaderUpdateTime int64
	Pending          []*Request
}

// Every heartbeat will also contain a list of all files in the system
type Request struct {
	ID       int
	Type     string
	SrcHost  string
	FileName string
}

// Server setup that will make initial connections
func ServerJoin(membership *Membership) {
	hostname, _ := os.Hostname()
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
}

// Goroutine that will send out heartbeats every half second.
func HeartbeatManager(membership *Membership) {
	hostname, _ := os.Hostname()
	ticker := time.NewTicker(1000 * time.Millisecond)
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

// Function called by the heartbeat manager that will send heartbeats to neighbors
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

// This writes the membership lists to the socket which is called by sendHeartbeats
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

// Loops through the membership and checks if any of the times on the nodes are past the
// allowed timeout. If the last time is 0 then the node has left the network. We call this
// after we send heartbeats because if the current node leaves, it sets its Data map enty
// to 0 and must propogate this out to the other nodes.
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

// Goroutine that constantly listens for incoming UDP calls
func ListenForUDP(ser *net.UDPConn, membership *Membership) {
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

		newMembership := &Membership{}
		err = json.Unmarshal(buffer[:readLen], &newMembership)
		if err != nil {
			log.Infof("Could not decode request! %s", err)
			return
		}

		processNewMembershipList(membership, newMembership)
		if newMembership.LeaderUpdateTime > membership.LeaderUpdateTime {
			membership.Pending = newMembership.Pending
		}
	}
}

// Loop through the new membership and update the timestamps in the current node.
func processNewMembershipList(membership *Membership, newMembership *Membership) {
	for i := 0; i < len(newMembership.List); i++ {
		nextHostname := newMembership.List[i]

		// If it finds a node that is not in the data map, add it to list and map
		if pingTime, contains := membership.Data[nextHostname]; !contains {
			membership.Data[nextHostname] = newMembership.Data[nextHostname]

			membership.List = append(membership.List, nextHostname)
			sort.Strings(membership.List)

			// If the time in the new list is 0, the node left the network
		} else if newMembership.Data[nextHostname] == 0 {
			membership.Data[nextHostname] = 0

			// If the new membership has a more recent time, update it
		} else if pingTime < newMembership.Data[nextHostname] {
			membership.Data[nextHostname] = newMembership.Data[nextHostname]

			// If the hostname is not in the list but it's in the data map,
			// it was removed from the list because it faile or left, and
			// we just recieved a new time indicating that it's rejoining.
			currTime := time.Now().UnixNano() / int64(time.Millisecond)
			if findHostnameIndex(membership, nextHostname) >= len(membership.List) &&
				currTime-newMembership.Data[nextHostname] < TIMEOUT_MS {

				membership.List = append(membership.List, nextHostname)
				sort.Strings(membership.List)
				log.Infof("Recieved updated time from node %s. Adding back to list", nextHostname)
			}
		}
	}

	// Update the time of the node who sent the list. Need to check if that node left
	// Because if a node leaves it will send a few heartbeats to other nodes to
	// Inform others that the node left the system
	if membership.Data[newMembership.SrcHost] != 0 {
		membership.Data[newMembership.SrcHost] = time.Now().UnixNano() / int64(time.Millisecond)
	}
}

// Search that will find the index of the hostname in the list
func findHostnameIndex(membership *Membership, hostName string) int {
	for i := 0; i < len(membership.List); i++ {
		if membership.List[i] == hostName {
			return i
		}
	}

	return len(membership.List)
}
