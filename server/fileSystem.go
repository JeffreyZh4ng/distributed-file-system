package server

import (
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"time"
)

var FINISHED_REQUEST_TTL int64 = 5000
var NUM_REPLICAS int = 4

// Local datastore that keeps track of the other nodes that have the same files
type LocalFileSystem struct {
	Files       map[string][]string
	UpdateTimes map[string]int64
}

// Need to keep a global file list and server response map
var ServerResponses map[string][]string
var LocalFiles *LocalFileSystem
var ClientRPCServer *rpc.Server
var ServerRPCServer *rpc.Server
var FileRPCServer *rpc.Server

// go routine that will handle requests and resharding of files from failed nodes
func FileSystemManager() {
	LocalFiles = &LocalFileSystem{
		Files: map[string][]string{},
		UpdateTimes: map[string]int64{},
	}

	go clientRequestListener()
	go serverResponseListener()
	go fileTransferListener()

	completedRequests := make(map[string]int64)
	ticker := time.NewTicker(1000 * time.Millisecond)

	for {
		<-ticker.C

		// Check if there are any requests in the membership list
		for _, request := range Membership.Pending {

			// If the request is in the complete list or the node doesnt have the file, continue
			_, isRequestFinished := completedRequests[request.ID]
			_, nodeContainsFile := LocalFiles.Files[request.FileName]
			if !nodeContainsFile || isRequestFinished {
				continue
			}

			// We let the delete fall through to the default case
			switch request.Type {
			case "Delete":
				deleteLocalFile(request.FileName)
			default:
				fileFoundRCP(request)
				break
			}

			completedRequests[request.ID] = time.Now().UnixNano() / int64(time.Millisecond)
		}

		findFailedNodes()
		for requestID, finishTime := range completedRequests {
			currTime := time.Now().UnixNano() / int64(time.Millisecond)
			if currTime-finishTime > FINISHED_REQUEST_TTL {
				delete(completedRequests, requestID)
			}
		}
	}
}

// Goroutine that will listen for incoming RPC requests made from any client
func clientRequestListener() {
	ClientRPCServer = rpc.NewServer()
	clientRequestRPC := new(ClientRequest)
	err := ClientRPCServer.Register(clientRequestRPC)
	if err != nil {
		log.Fatalf("Format of service ClientRequest isn't correct. %s", err)
	}

	// Weird thing we have to do to allow more than one RPC server running
	oldMux := http.DefaultServeMux
    mux := http.NewServeMux()
    http.DefaultServeMux = mux
	ClientRPCServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	http.DefaultServeMux = oldMux

	listener, _ := net.Listen("tcp", ":"+CLIENT_RPC_PORT)

	go http.Serve(listener, nil)
}

// Goroutine that will listen for incoming RPC calls from other servers
func serverResponseListener() {
	ServerRPCServer = rpc.NewServer()
	serverResponseRPC := new(ServerCommunication)
	err := ServerRPCServer.Register(serverResponseRPC)
	if err != nil {
		log.Fatalf("Format of service ServerCommunication isn't correct. %s", err)
	}

	// Weird thing we have to do to allow more than one RPC server running
	oldMux := http.DefaultServeMux
    mux := http.NewServeMux()
    http.DefaultServeMux = mux
	ServerRPCServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	http.DefaultServeMux = oldMux

	listener, _ := net.Listen("tcp", ":"+SERVER_RPC_PORT)

	go http.Serve(listener, nil)
}

// Goroutine that will listen for incoming RPC connection to transfer a file
func fileTransferListener() {
	FileRPCServer = rpc.NewServer()
	fileTransferRPC := new(FileTransfer)
	err := FileRPCServer.Register(fileTransferRPC)
	if err != nil {
		log.Fatalf("Format of service ServerCommunication isn't correct. %s", err)
	}

	// Weird thing we have to do to allow more than one RPC server running
	oldMux := http.DefaultServeMux
    mux := http.NewServeMux()
    http.DefaultServeMux = mux
	FileRPCServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	http.DefaultServeMux = oldMux

	listener, _ := net.Listen("tcp", ":"+FILE_RPC_PORT)

	go http.Serve(listener, nil)
}

// Function that deletes data for a file from the server and the localFiles struct
func deleteLocalFile(fileName string) {
	delete(LocalFiles.Files, fileName)
	delete(LocalFiles.UpdateTimes, fileName)

	err := os.Remove(SERVER_FOLDER_NAME + fileName)
	if err != nil {
		log.Infof("Unable to remove file %s from local node!", fileName)
		return
	}
}

// Function that will send an RPC call to the leader indicating that the request has been fulfilled
// This RPC will only be sent out if the current node is the leader of the FileGroup (Lowest ID)
func fileFoundRCP(request *Request) {
	hostname, _ := os.Hostname()
	fileGroup, _ := LocalFiles.Files[request.FileName]
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

		fileGroupAliveNodes := []string{}
		for _, node := range fileGroup {

			if _, contains := aliveNodes[node]; contains {
				fileGroupAliveNodes = append(fileGroupAliveNodes, node)
			}
		}

		// If there are less than NUM_REPLICAS of a file, and the current node is the groupMaster, reshard
		if len(fileGroupAliveNodes) < NUM_REPLICAS && fileGroupAliveNodes[0] == hostname {
			reshardFiles(fileName, fileGroupAliveNodes)
		}
	}
}

// This will randomly pick another node that doesnt have the file to reshard the file to
func reshardFiles(fileName string, fileGroupAliveNodes []string) {
	var newFileGroup []string
	copy(newFileGroup, fileGroupAliveNodes)
	reshardTargetNodes := []string{}

	for i := 0; i < NUM_REPLICAS-len(fileGroupAliveNodes); i++ {

		// Disgusting random function that will loop until it finds a node not in the runningNodes list
		randIndex := 0

		for {
			randIndex = rand.Intn(len(Membership.List))
			nodeName := Membership.List[randIndex]
			for i := 0; i < len(fileGroupAliveNodes); i++ {
				if fileGroupAliveNodes[i] == nodeName {
					continue
				}
			}

			break
		}

		randomNode := Membership.List[randIndex]
		reshardTargetNodes = append(reshardTargetNodes, randomNode)
		newFileGroup = append(newFileGroup, randomNode)
		sort.Strings(newFileGroup)
	}

	loadedFile, _ := ioutil.ReadFile(SERVER_FOLDER_NAME + fileName)
	fileTransferArgs := &FileTransferRequest{
		FileName: fileName,
		FileGroup: newFileGroup,
		Data: loadedFile,
	}
	for _, node := range reshardTargetNodes {
		fileTransferRPC(node, fileTransferArgs)
	}

	updateFileGroupArgs := &UpdateFileGroupRequest{
		FileName: fileName,
		FileGroup: newFileGroup,
	}
	for _, node := range fileGroupAliveNodes {
		updateFileGroupRPC(node, updateFileGroupArgs)
	}
}

// Helper that will call the SendFile RPC and will send a file to a node
func fileTransferRPC(transferHost string, fileTransferArgs *FileTransferRequest) {
	client, err := rpc.DialHTTP("tcp", transferHost+":"+FILE_RPC_PORT)
	if err != nil {
		log.Fatalf("Error in dialing. %s", err)
	}

	err = client.Call("FileTransfer.SendFile", fileTransferArgs, nil)
	if err != nil {
		log.Fatalf("error in ServerCommunication", err)
	}
}

// Helper that will call the UpdateFileGroup RPC and will update the FileGroup of a file at a node
func updateFileGroupRPC(updateHost string, updateFileGroupArgs *UpdateFileGroupRequest) {
	client, err := rpc.DialHTTP("tcp", updateHost+":"+FILE_RPC_PORT)
	if err != nil {
		log.Fatalf("Error in dialing. %s", err)
	}

	err = client.Call("FileTransfer.UpdateFileGroup", updateFileGroupArgs, nil)
	if err != nil {
		log.Fatalf("error in ServerCommunication", err)
	}
}