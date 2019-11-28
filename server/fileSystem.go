package server

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"time"
)

// Local datastore that keeps track of the other nodes that have the same files
type LocalFiles struct {
	Files       map[string][]string
	UpdateTimes map[string]int64
}

var FINISHED_REQUEST_TTL int = 5000
var NUM_REPLICAS int = 4

// Need to keep a global file list and server response map
var ServerResponses map[string]string
var localFiles *LocalFiles

// go routine that will handle requests and resharding of files from failed nodes
func FileSystemManager() {
	go clientRequestListener()
	go serverResponseListener()
	go fileTransferListener()

	// localFiles = &server.LocalFiles{
	// 	Files: map[string][]string{},
	// 	UpdateTimes: map[string]int64{},
	// }

	// completedRequests := make(map[int]int64)
	// ticker := time.NewTicker(1000 * time.Millisecond)

	// for {
	// 	<-ticker.C

	// 	// Check if there are any requests in the membership list
	// 	for _, request := range membership.Pending {

	// 		// If the request is in the complete list then continue
	// 		if _, contains := completedRequests[request.ID]; contains {
	// 			continue
	// 		}

	// 		requestCompleted := false
	// 		switch request.Type {
	// 		case "put":
	// 			requestCompleted = serverHandlePut(membership, localFiles, request)
	// 		case "get":
	// 			requestCompleted = serverHandleGet(membership, localFiles, request)
	// 		case "delete":
	// 			requestCompleted = serverHandleDelete(membership, localFiles, request)
	// 		case "ls":
	// 			requestCompleted = serverHandleLs(membership, localFiles, request)
	// 		}

	// 		if requestCompleted {
	// 			completedRequests[request.ID] = time.Now().UnixNano() / int64(time.Millisecond)
	// 		}
	// 	}

	// 	findFailedNodes(membership, localFiles)
	// 	for requestID, finishTime := range completedRequests {
	// 		currTime := time.Now().UnixNano() / int64(time.Millisecond)
	// 		if currTime-finishTime > TIMEOUT_MS {
	// 			delete(completedRequests, requestID)
	// 		}
	// 	}
	// }
}

// Goroutine that will listen for incoming RPC requests made from any client
func clientRequestListener() {
	clientRequestRPC := new(ClientRequest)
	err := rpc.Register(clientRequestRPC)
	if err != nil {
		log.Fatalf("Format of service ClientRequest isn't correct. %s", err)
	}

	rpc.HandleHTTP()
	listener, _ := net.Listen("tcp", ":"+CLIENT_RPC_PORT)
	defer listener.Close()

	http.Serve(listener, nil)
}

// Goroutine that will listen for incoming RPC calls from other servers
func serverResponseListener() {
	serverResponseRPC := new(ServerCommunication)
	err := rpc.Register(serverResponseRPC)
	if err != nil {
		log.Fatalf("Format of service ServerCommunication isn't correct. %s", err)
	}

	rpc.HandleHTTP()
	listener, _ := net.Listen("tcp", ":"+SERVER_RPC_PORT)
	defer listener.Close()

	http.Serve(listener, nil)
}

// Goroutine that will listen for incoming RPC connection to transfer a file
func fileTransferListener() {
	serverResponseRPC := new(ServerCommunication)
	err := rpc.Register(serverResponseRPC)
	if err != nil {
		log.Fatalf("Format of service ServerCommunication isn't correct. %s", err)
	}

	rpc.HandleHTTP()
	listener, _ := net.Listen("tcp", ":"+FILE_RPC_PORT)
	defer listener.Close()

	http.Serve(listener, nil)
}

// Helper method that will contact a specified node and send a node message
func contactNode(nodeHostName string, leaderMessage NodeMessage) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", nodeHostName+":"+SERVER_PORT)
	if err != nil {
		log.Info("Could not resolve the hostname!")
		return
	}

	socket, err := net.DialTCP("tcp", nil, tcpAddr)
	defer socket.Close()
	if err != nil {
		log.Info("Server could not dial the client!")
		return
	}

	jsonMessage, _ := json.Marshal(leaderMessage)
	socket.Write(jsonMessage)
}

// Helper that establishes a TCP connection
func establishTCP(hostname string) *net.TCPConn {
	tcpAddr, err := net.ResolveTCPAddr("tcp", hostname+":"+SERVER_PORT)
	if err != nil {
		log.Info("Could not resolve the hostname!")
		return nil
	}

	socket, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		log.Info("Server could not dial the client!")
		return nil
	}

	return socket
}

func findFailedNodes(membership *Membership, localFiles *LocalFiles) {
	hostname, _ := os.Hostname()
	currentFiles := map[string]int{}
	for _, host := range membership.List {
		currentFiles[host] = 0
	}

	// Loop through every node in every file stored in the local system
	for fileName, fileGroup := range localFiles.Files {

		runningNodes := []string{}
		for _, node := range fileGroup {

			if _, contains := currentFiles[node]; !contains {
				runningNodes = append(runningNodes, node)
			}
		}

		// If the current node is the lowest ID node that has not filed, reshard
		if runningNodes[0] == hostname {
			reshardFiles(membership, runningNodes, fileName)
		}
	}
}

func reshardFiles(membership *Membership, runningNodes []string, fileName string) {
	hostname, _ := os.Hostname()
	for i := 0; i < NUM_REPLICAS-len(runningNodes); i++ {

		// Disgusting random function that will loop until it finds a node not in the runningNodes list
		randIndex := 0
		for {
			randIndex = rand.Intn(len(membership.List))
			nodeName := membership.List[randIndex]
			for i := 0; i < len(runningNodes); i++ {
				if runningNodes[i] == nodeName {
					continue
				}
			}

			break
		}

		loadedFile, _ := ioutil.ReadFile(PARENT_DIR + "/" + fileName)
		nodeMessage := NodeMessage{
			MsgType:  "ReshardRequest",
			FileName: fileName,
			SrcHost:  hostname,
			Data:     loadedFile,
		}


		randomNode := membership.List[randIndex]
		socket := establishTCP(randomNode)
		if socket == nil {
			log.Info("Could not establish TCP while resharding")
		}

		jsonMessage, _ := json.Marshal(nodeMessage)
		_, err := socket.Write(jsonMessage)
		if err != nil {
			log.Info("Could not write file while resharding")
		}
	}
}
