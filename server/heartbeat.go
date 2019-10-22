package server

import (
	log "github.com/sirupsen/logrus"
	"encoding/json"
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
type Membership struct {
	SrcHost string
	Data    map[string]int64
	List    []string
	Store   *Directory
}

func ServerJoin(membership *Membership) {
	// If the node is not the introducer node, send a heartbeat to the 0th node
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
func HeartbeatManager(membership *Membership, leaveSignal chan int) {
	hostname, _ := os.Hostname()
	ticker := time.NewTicker(1000 * time.Millisecond)
	var wg sync.WaitGroup

	for {
		// This will block until the ticker sends a value to the chan
		<-ticker.C
		wg.Add(1)
		go sendHeartbeats(membership, &wg)
		wg.Wait()

		// If we get a leave signal from the channel, leave 
		select {
		case signal := <- leaveSignal:
			log.Info("Caught leave signal ", signal)
			return
		default:
			// Do nothing	
		}

		// If the current node has not left the network then update its time
		membership.Data[hostname] = time.Now().UnixNano() / int64(time.Millisecond)
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

		processNewMembershipList(buffer, readLen, membership)
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

		// If it finds a node that is not in the data map, add it to the list and map.
		if pingTime, contains := membership.Data[nextHostname]; !contains {
			membership.Data[nextHostname] = newMembership.Data[nextHostname]

			membership.List = append(membership.List, nextHostname)
			sort.Strings(membership.List)

			// If the current node left the network or we get a UDP request that indicates the node
			// left the network then set the time in the data to 0.
		} else if membership.Data[nextHostname] == 0 || newMembership.Data[nextHostname] == 0 {
			membership.Data[nextHostname] = 0

			// If the stored ping time is less than the UDP request time, update the time
		} else if pingTime < newMembership.Data[nextHostname] {
			membership.Data[nextHostname] = newMembership.Data[nextHostname]

			// If the hostname is not in the list but it's in the data map, add it back
			// to the list. This means it was removed from the list because it failed
			// or left, and we just recieved a new time indicating that it's rejoining.
			currTime := time.Now().UnixNano() / int64(time.Millisecond)
			if findHostnameIndex(membership, nextHostname) >= len(membership.List) &&
				currTime-newMembership.Data[nextHostname] < TIMEOUT_MS {

				membership.List = append(membership.List, nextHostname)
				sort.Strings(membership.List)
				log.Infof("Recieved updated time from node %s. Adding back to list", nextHostname)
			}
		}
	}

	// If there was an update to the filesystem, get the change
	if newMembership.Store.LastUpdate > membership.Store.LastUpdate {
		membership.Store = newMembership.Store
	}

	// Update the time of the node who sent the list. We need to check if the node has not left
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
