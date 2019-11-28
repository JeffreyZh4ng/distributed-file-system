package server

import (
	"net/rpc"
	"strconv"
	"time"
)

// Hold arguments to be returned to the client
type ClientResponseArgs struct {
	Success bool,
	HostList []string,
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
var REQUEST_TIMEOUT int = 5000

// Global that keeps track of how many requests have been created by this node
var requestCount int = 0

func (t *ClientRequest) Put(requestFile string, response *ClientResponseArgs) error {
	// This function will look for the file in the system. If it doesnt exist,
	// The leader will return the nodes to write to and if the user need confirmation
	// The client will then RPC connect to the nodes and write the file the client
	// Will handle the confirmation
	success, hostList := handleClientRequest("Put", requestFile)
	*response.Success = success

	// If the file was not found, pick four random nodes to shard the file to
	if !success {
		randomHostList = []string

		for {
			randIndex = rand.Intn(len(membership.List))
			nodeName := membership.List[randIndex]

			// If the current random pick matches one that was already picked, continue
			for i := 0; i < len(randomHostList); i++ {
				if randomHostList[i] == nodeName {
					continue
				}
			}
			
			randomHostList = append(randomHostList, nodeName)
			if len(randomHostList) == 4 {
				*response.hostList = randomHostList
				return nil
			}
		}
	} 
	
	*response.hostList = hostList
	return nil
}

func (t *ClientRequest) Get(requestFile string, response *ClientResponseArgs) error {
	// This function will return a response of which node has the specified file
	// The client will then connect to that server with RPC to retrieve the file
	success, hostList := handleClientRequest("Get", requestFile)
	*response.Success = success
	*response.HostList = hostList

	return nil
}

func (t *ClientRequest) Delete(requestFile string, response *ClientResponseArgs) error {
	success, hostList := handleClientRequest("Delete", requestFile)
	*response.Success = success
	*response.HostList = []string

	return nil
}

func (t *ClientRequest) List(requestFile string, response *ClientResponseArgs) error {
	success, hostList := handleClientRequest("List", requestFile)
	*response.Success = success
	*response.HostList = hostList

	return nil
}

func handleClientRequest(requestType string, requestFile string) (success bool, hostList []string) {
	// Will create a unique ID name with the curent VM name and a counter for how many client
	// Requests have been created from this node
	hostname, _ := os.Hostname()
	requestId := hostname + strconv.Itoa(requestCount)
	requestCount++

	request := &Request{
		ID:       requestId,
		Type:     requestType,
		SrcHost:  hostname,
		FileName: requestFile,
	}
	Membership.Pending = append(Membership.Pending, request)

	// Will check if the node has recieved a response from another server 
	// Indicating that that node has the file
	startTime := time.Now().UnixNano() / int64(time.Millisecond)
	ticker := time.NewTicker(500 * time.Millisecond)
	
	for {
		<-ticker.C

		// If the leader recieved a response from a server that the file was found
		if hostList, contains := ServerResponses[requestId]; contains {
			delete(ServerResponses, requestId)
			return hostList
		}

		if currTime-startTime > REQUEST_TIMEOUT {
			break
		}
	}

	return []string
}