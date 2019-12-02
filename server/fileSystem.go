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

var NUM_REPLICAS int = 4

// Local datastore that keeps track of the other nodes that have the same files
type LocalFileSystem struct {
	Files       map[string][]string
	UpdateTimes map[string]int64
}

// Need to keep a global file list and server response map
var ServerResponses map[string][]string
var LocalFiles *LocalFileSystem

// go routine that will handle requests and resharding of files from failed nodes
func FileSystemManager() {
	ServerResponses = map[string][]string{}
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
			fileFoundRPC(request)
			if request.Type == "Delete" {
				deleteLocalFile(request.FileName)
			}

			completedRequests[request.ID] = time.Now().UnixNano() / int64(time.Millisecond)
		}

		findFailedNodes()
	}
}

// Goroutine that will listen for incoming RPC requests made from any client
func clientRequestListener() {
	server := rpc.NewServer()
	clientRequestRPC := new(ClientRequest)
	err := server.Register(clientRequestRPC)
	if err != nil {
		log.Fatalf("Format of service ClientRequest isn't correct. %s", err)
	}

	// Weird thing we have to do to allow more than one RPC server running
	oldMux := http.DefaultServeMux
    mux := http.NewServeMux()
    http.DefaultServeMux = mux
	server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	http.DefaultServeMux = oldMux

	listener, _ := net.Listen("tcp", ":"+CLIENT_RPC_PORT)

	go http.Serve(listener, mux)
}

// Goroutine that will listen for incoming RPC calls from other servers
func serverResponseListener() {
	server := rpc.NewServer()
	serverResponseRPC := new(ServerCommunication)
	err := server.Register(serverResponseRPC)
	if err != nil {
		log.Fatalf("Format of service ServerCommunication isn't correct. %s", err)
	}

	// Weird thing we have to do to allow more than one RPC server running
	oldMux := http.DefaultServeMux
    mux := http.NewServeMux()
    http.DefaultServeMux = mux
	server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	http.DefaultServeMux = oldMux

	listener, _ := net.Listen("tcp", ":"+SERVER_RPC_PORT)

	go http.Serve(listener, mux)
}

// Goroutine that will listen for incoming RPC connection to transfer a file
func fileTransferListener() {
	server := rpc.NewServer()
	fileTransferRPC := new(FileTransfer)
	err := server.Register(fileTransferRPC)
	if err != nil {
		log.Fatalf("Format of service fileTransfer isn't correct. %s", err)
	}

	// Weird thing we have to do to allow more than one RPC server running
	oldMux := http.DefaultServeMux
    mux := http.NewServeMux()
    http.DefaultServeMux = mux
	server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	http.DefaultServeMux = oldMux

	listener, _ := net.Listen("tcp", ":"+FILE_RPC_PORT)

	go http.Serve(listener, mux)
}

// Function that deletes data for a file from the server and the localFiles struct
func deleteLocalFile(fileName string) {
	delete(LocalFiles.Files, fileName)
	delete(LocalFiles.UpdateTimes, fileName)
	log.Infof("File %s deleted from the server!", fileName)

	err := os.Remove(SERVER_FOLDER_NAME + fileName)
	if err != nil {
		log.Infof("Unable to remove file %s from local node!", fileName)
		return
	}
}

// Function that will send an RPC call to the leader indicating that the request has been fulfilled
// This RPC will only be sent out if the current node is the leader of the FileGroup (Lowest ID)
func fileFoundRPC(request *Request) {
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
		FileName: request.FileName,
		HostList: fileGroup,
	}

	log.Infof("File %s for ID: %s found on this server!", request.FileName, request.ID)
	err = client.Call("ServerCommunication.FileFound", args, nil)
	if err != nil {
		log.Fatalf("error in ServerCommunication", err)
	}
}

// This will find all the files that need to be resharded to another node
func findFailedNodes() {

	// Populate a map to make alive node lookup easier
	aliveNodes := map[string]int{}
	for _, host := range Membership.List {
		aliveNodes[host] = 0
	}

	// Loop through every node in every file stored in the local system
	hostname, _ := os.Hostname()
	for fileName, fileGroup := range LocalFiles.Files {

		fileGroupAliveNodes := []string{}
		for _, node := range fileGroup {

			if _, contains := aliveNodes[node]; contains {
				fileGroupAliveNodes = append(fileGroupAliveNodes, node)
			}
		}

		// If there are less than NUM_REPLICAS of a file, and the current node is the fileMaster, reshard
		if len(fileGroupAliveNodes) < NUM_REPLICAS && fileGroupAliveNodes[0] == hostname {
			reshardFiles(fileName, fileGroupAliveNodes)
		}
	}
}

// This will randomly pick nodes that doesnt have the file to reshard the file to them
func reshardFiles(fileName string, fileGroupAliveNodes []string) {
	var newFileGroup []string
	copy(newFileGroup, fileGroupAliveNodes)
	rand.Seed(time.Now().UnixNano())

	// Disgusting random function that will loop until it finds a node not in the runningNodes list
	newGroupMembers := []string{}
	for {
		randIndex := rand.Intn(len(Membership.List))
		nodeName := Membership.List[randIndex]

		duplicate := false
		for i := 0; i < len(newFileGroup); i++ {
			if newFileGroup[i] == nodeName {
				duplicate = true
				break
			}
		}

		if duplicate {
			continue
		}

		newFileGroup = append(newFileGroup, nodeName)
		newGroupMembers = append(newGroupMembers, nodeName)
		if len(newFileGroup) == NUM_REPLICAS {
			sort.Strings(newFileGroup)
			break
		}
	}

	// Update the fileGroup for the original owners of the file
	updateFileGroupArgs := &ServerRequestArgs{
		ID: "",
		FileName: fileName,
		HostList: newFileGroup,
	}
	for _, node := range fileGroupAliveNodes {
		updateFileGroupRPC(node, updateFileGroupArgs)
	}

	// Send the file and the new group over to the new members of the fileGroup
	loadedFile, _ := ioutil.ReadFile(SERVER_FOLDER_NAME + fileName)
	fileTransferArgs := &FileTransferRequest{
		FileName: fileName,
		FileGroup: newFileGroup,
		Data: loadedFile,
	}
	for _, node := range newGroupMembers {
		fileTransferRPC(node, fileTransferArgs)
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
		log.Fatalf("error in fileTransfer", err)
	}
}

// Helper that will call the UpdateFileGroup RPC and will update the FileGroup of a file at a node
func updateFileGroupRPC(updateHost string, updateFileGroupArgs *ServerRequestArgs) {
	client, err := rpc.DialHTTP("tcp", updateHost+":"+FILE_RPC_PORT)
	if err != nil {
		log.Fatalf("Error in dialing. %s", err)
	}

	err = client.Call("ServerCommunication.UpdateFileGroup", updateFileGroupArgs, nil)
	if err != nil {
		log.Fatalf("error in ServerCommunication", err)
	}
}