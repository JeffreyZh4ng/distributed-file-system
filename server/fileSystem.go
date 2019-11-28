package server

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"sort"
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
	localFiles = &server.LocalFiles{
		Files: map[string][]string{},
		UpdateTimes: map[string]int64{},
	}

	go clientRequestListener()
	go serverResponseListener()
	go fileTransferListener()

	completedRequests := make(map[int]int64)
	ticker := time.NewTicker(1000 * time.Millisecond)

	for {
		<-ticker.C

		// Check if there are any requests in the membership list
		for _, request := range membership.Pending {

			// If the request is in the complete list or the node doesnt have the file, continue
			_, isRequestFinished := completedRequests[request.ID]
			nodeGroup, nodeContainsFile := LocalFiles.Files[request.FileName]
			if !nodeContainsFile || isRequestFinished {
				continue
			}

			// We let the delete fall through to the default case
			switch request.Type {
			case "Delete":
				deleteLocalFile(request.FileName)
			case default:
				serverHandleLs(request)
				break
			}

			completedRequests[request.ID] = time.Now().UnixNano() / int64(time.Millisecond)
		}

		findFailedNodes()
		for requestID, finishTime := range completedRequests {
			currTime := time.Now().UnixNano() / int64(time.Millisecond)
			if currTime-finishTime > TIMEOUT_MS {
				delete(completedRequests, requestID)
			}
		}
	}
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

// Function that deletes data for a file from the server and the localFiles struct
func deleteLocalFile(fileName string) {
	delete(localFiles.Files, fileName)
	delete(localFiles.UpdateTimes, fileName)

	err := os.Remove(FOLDER_NAME + fileName)
	if err != nil {
		log.Infof("Unable to remove file %s from local node!", fileName)
		return
	}
}

// Function that will send an RPC call to the leader indicating that the request has been fulfilled
// This RPC will only be sent out if the current node is the leader of the nodeGroup (Lowest ID)
func fileFoundRCP(request *Request) {
	hostname, _ := os.Hostname()
	fileGroup, _ := localFiles.Files[request.FileName]
	if fileGroup[0] != hostname {
		return
	}

	client, err := rpc.DialHTTP("tcp", request.SrcHost+":"+SERVER_RPC_PORT)
	if err != nil {
		log.Fatalf("Error in dialing. %s", err)
	}

	args := &ServerRequestArgs{
		ID: request.ID,
		HostList: fileGroup,
	}

	err = client.Call("ServerCommunication.FileFound", args, nil)
	if err != nil {
		log.Fatalf("error in ServerCommunication", err)
	}
}

// This will find all the files that need to be resharded to another node
func findFailedNodes() {
	hostname, _ := os.Hostname()
	aliveNodes := map[string]int{}
	for _, host := range Membership.List {
		aliveNodes[host] = 0
	}

	// Loop through every node in every file stored in the local system
	for fileName, fileGroup := range LocalFiles.Files {

		nodeGroupAliveNodes := []string{}
		for _, node := range fileGroup {

			if _, contains := aliveNodes[node]; contains {
				nodeGroupAliveNodes = append(fileGroupAliveNodes, node)
			}
		}

		// If there are less than NUM_REPLICAS of a file, and the current node is the groupMaster, reshard
		if len(fileGroupAliveNodes) < NUM_REPLICAS && nodeGroupAliveNodes[0] == hostname {
			reshardFiles(fileName, nodeGroupAliveNodes)
		}
	}
}

// This will randomly pick another node that doesnt have the file to reshard the file to
func reshardFiles(fileName string, nodeGroupAliveNodes []string) {
	hostname, _ := os.Hostname()

	var newNodeGroup []string
	newNodeGroup = copy(nodeNodeGroup, nodeGroupAliveNodes)
	reshardTargetNodes := []string{}

	for i := 0; i < NUM_REPLICAS-len(nodeGroupAliveNodes); i++ {

		// Disgusting random function that will loop until it finds a node not in the runningNodes list
		randIndex := 0

		for {
			randIndex = rand.Intn(len(membership.List))
			nodeName := Membership.List[randIndex]
			for i := 0; i < len(nodeGroupAliveNodes); i++ {
				if nodeGroupAliveNodes[i] == nodeName {
					continue
				}
			}

			break
		}

		randomNode := Membership.List[randIndex]
		reshardTargetNodes = append(reshardTargetNodes, randomNode)
		newNodeGroup = append(newNodeGroup, randomNode)
		sort.Strings(newNodeGroup)
	}

	loadedFile, _ := ioutil.ReadFile(PARENT_DIR + "/" + fileName)
	fileTransferArgs := &FileTransferRequest{
		FileName: fileName,
		FileGroup: newNodeGroup,
		Data: loadedFile,
	}
	for _, node := range reshardTargetNodes {
		fileTransferRPC(fileTransferArgs)
	}

	updateNodeGroupArgs := &UpdateNodeGroupRequest{
		FileName: fileName,
		NodeGroup: newNodeGroup,
	}
	for _, node := range nodeGroupAliveNodes {
		updateNodeGroupRPC(updateNodeGroupArgs)
	}
}

// Helper that will call the SendFile RPC and will send a file to a node
func fileTransferRPC(transferHost string, fileTransferArgs *FileTransferRequest) {
	client, err := rpc.DialHTTP("tcp", rtransferHost+":"+FILE_RPC_PORT)
	if err != nil {
		log.Fatalf("Error in dialing. %s", err)
	}

	err = client.Call("FileTransfer.SendFile", fileTransferArgs, nil)
	if err != nil {
		log.Fatalf("error in ServerCommunication", err)
	}
}

// Helper that will call the UpdateNodeGroup RPC and will update the nodeGroup of a file at a node
func updateNodeGroupRPC(updateHost string, updateNodeGroupArgs *UpdateNodeGroupRequest) {
	client, err := rpc.DialHTTP("tcp", updateHost+":"+FILE_RPC_PORT)
	if err != nil {
		log.Fatalf("Error in dialing. %s", err)
	}

	err = client.Call("FileTransfer.UpdateNodeGroup", updateNodeGroupArgs, nil)
	if err != nil {
		log.Fatalf("error in ServerCommunication", err)
	}
}