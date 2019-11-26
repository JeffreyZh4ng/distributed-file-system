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
var requestCount int = 0

func (t *ClientRequest) Put(requestFile string, response *ClientResponseArgs) error {
	// This function will look for the file in the system. If it doesnt exist,
	// The leader will return the nodes to write to and if the user need confirmation
	// The client will then RPC connect to the nodes and write the file the client
	// Will handle the confirmation
	return nil
}

func (t *ClientRequest) Get(requestFile string, response *ClientResponseArgs) error {
	// This function will return a response of which node has the specified file
	// The client will then connect to that server with RPC to retrieve the file
	return nil
}

func (t *ClientRequest) Delete(requestFile string, response *ClientResponseArgs) error {
	// This will call a function that will add the request into the queue then it will wait
	// For a response. After a timeout, the function will return with the response of the request

	// Add the request into the the pending request list inside of membeship
	hostname, _ := os.Hostname()
	requestId := hostname + strconv.Itoa(requestCount)
	requestCount++

	request := &Request{
		ID:       requestId,
		Type:     "Delete",
		SrcHost:  hostname,
		FileName: requestFile,
	}
	Membership.Pending = append(Membership.Pending, request)

	// Start listening for incoming RPC calls. If none arrive within the specified timeout, assume
	// No file was found. fileSystem.go will be listening to the different ports. We need to make a function
	// in that file that we call from here that listens to that connection for around five seconds
	startTime := time.Now().UnixNano() / int64(time.Millisecond)
	ticker := time.NewTicker(500 * time.Millisecond)
	
	for {
		<-ticker.C

		// If the leader recieved a response from a server that the file was found
		if hostname, contains := ServerResponses[]; contains {
			*response.Success = true
			*response.Hostlist = [hostname]
			
			return nil
		}

		if currTime-startTime > REQUEST_TIMEOUT {
			break
		}
	}

	*response.Success = false
	*response.HostList = []

	return nil
}

func (t *ClientRequest) List(requestFile string response *ClientResponseArgs) error {
	// This function will get a list of all the files from a server
	// It will return if the file was found
	return nil
}