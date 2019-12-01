package server

import (
	log "github.com/sirupsen/logrus"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"time"
)

// Hold arguments to be returned to the client
type ClientResponseArgs struct {
	Success bool
	HostList []string
}

// Represents service Request
type ClientRequest int

// Struct for the request buffer within each heartbeat
type Request struct {
	ID       string
	Type     string
	SrcHost  string
	FileName string
}

var CLIENT_RPC_PORT string = "5000"
var REQUEST_TIMEOUT int64 = 3000

// Global that keeps track of how many requests have been created by this node
var requestCount int = 1000

func (t *ClientRequest) Put(requestFile string, response *ClientResponseArgs) error {
	// This function will look for the file in the system. If it doesnt exist,
	// The leader will return the nodes to write to and if the user need confirmation
	// The client will then RPC connect to the nodes and write the file the client
	// Will handle the confirmation
	log.Infof("Server recieved Put for file %s", requestFile)
	success, hostList := handleClientRequest("Put", requestFile)

	// We can change this to indicate if it was within the grace period
	response.Success = success

	// If the file was not found, pick four random nodes to shard the file to
	if !success {
		randomHostList := []string{}

		for {
			rand.Seed(time.Now().UnixNano())
			randIndex := rand.Intn(len(Membership.List))
			nodeName := Membership.List[randIndex]

			// If the current random pick matches one that was already picked, continue
			duplicate := false
			for i := 0; i < len(randomHostList); i++ {
				if randomHostList[i] == nodeName {
					duplicate = true
					break
				}
			}
			
			if duplicate {
				continue
			}

			randomHostList = append(randomHostList, nodeName)
			if len(randomHostList) == 4 || len(randomHostList) == len(Membership.List) {
				sort.Strings(randomHostList)
				response.HostList = randomHostList
				return nil
			}
		}
	} 
	
	response.HostList = hostList
	return nil
}

func (t *ClientRequest) Get(requestFile string, response *ClientResponseArgs) error {
	log.Infof("Server recieved Get for file %s", requestFile)
	success, hostList := handleClientRequest("Get", requestFile)
	response.Success = success
	response.HostList = hostList

	return nil
}

func (t *ClientRequest) Delete(requestFile string, response *ClientResponseArgs) error {
	log.Infof("Server recieved Delete for file %s", requestFile)
	success, _ := handleClientRequest("Delete", requestFile)
	response.Success = success
	response.HostList = []string{}

	return nil
}

func (t *ClientRequest) List(requestFile string, response *ClientResponseArgs) error {
	log.Infof("Server recieved Ls for file %s", requestFile)
	success, hostList := handleClientRequest("List", requestFile)
	response.Success = success
	response.HostList = hostList

	return nil
}

func handleClientRequest(requestType string, requestFile string) (success bool, hostList []string) {
	// Will create a unique ID name with the curent VM name and a counter for how many client
	// Requests have been created from this node
	hostname, _ := os.Hostname()
	requestId := hostname+"["+strconv.Itoa(requestCount)+"]"
	requestCount++

	request := &Request{
		ID:       requestId,
		Type:     requestType,
		SrcHost:  hostname,
		FileName: requestFile,
	}
	log.Infof("Adding %s request, ID: %s to pending bus", requestType, requestId)
	Membership.Pending = append(Membership.Pending, request)
	Membership.RequestUTime = time.Now().UnixNano() / int64(time.Millisecond)

	// Will check if the node has recieved a response from another server 
	// Indicating that that node has the file
	startTime := time.Now().UnixNano() / int64(time.Millisecond)
	ticker := time.NewTicker(500 * time.Millisecond)
	
	for {
		<-ticker.C

		// If the leader recieved a response from a server that the file was found
		if hostList, contains := ServerResponses[requestId]; contains {
			delete(ServerResponses, requestId)
			removeRequest(requestId)
			log.Infof("Found file %s in the sdfs", requestFile)
			return true, hostList
		}

		currTime := time.Now().UnixNano() / int64(time.Millisecond)
		if currTime-startTime > REQUEST_TIMEOUT {
			delete(ServerResponses, requestId)
			removeRequest(requestId)
			log.Infof("Could not find file %s in the sdfs!", requestFile)
			break
		}
	}

	return false, []string{}
}

// Helper that will remove a pending request and update the RequestUTime
func removeRequest(requestId string) {
	for idx, request := range Membership.Pending {
		if request.ID == requestId {
			Membership.Pending = append(Membership.Pending[:idx], Membership.Pending[idx+1:]...)
			Membership.RequestUTime = time.Now().UnixNano() / int64(time.Millisecond)
			return
		}
	}
}