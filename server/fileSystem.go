package server

import (
	"ioutil"
	"math/rand"
	"net"
	"os"
	"time"
	"strings"
)

// Local datastore that keeps track of the other nodes that have the same files
type LocalFiles struct {
	Files map[string][]string
	UpdateTimes map[string]int64
}

type PendingResponse struct {
	RequestID int
	Timestamp int64
	NodeList []string
}

var PARENT_DIR string = "nodeFiles"
var NODE_PORT_NUM string = "6000"
var FINISHED_REQUEST_TTL int = 5000
var NUM_REPLICAS int = 4

// go routine that will handle requests and resharding of files from failed nodes
func FileSystemManager(membership *Membership, localFiles *LocalFiles) {
	hostname, _ := os.Hostname()
	completedRequests := map[int]int64
	ticker := time.NewTicker(1000 * time.Millisecond)

	for {
		<-ticker.C

		// Check if there are any requests in the membership list
		for _, request := range membership.Pending {
			
			// If the request is in the complete list then continue
			if _, contains := completedRequests[request.RequestID]; contains {
				continue
			}

			requestCompleted := false
			switch request.Type {
			case "put":
				requestCompleted = serverHandlePut(membership, localFiles, request)	
			case "get":
				requestCompleted = serverHandleGet(membership, localFiles, request)
			case "delete":
				requestCompleted = serverHandleDelete(membership, localFiles, request)
			case "ls":
				requestCompleted = serverHandleLs(membership, localFiles, request)
			}

			if requestCompleted {
				completedRequests[request.RequestID] = time.Now().UnixNano() / int64(time.Millisecond)
			}
		}

		findFailedNodes(membership, localFiles)
		for requestID, finishTime := range completedRequests {
			currTime := time.Now().UnixNano() / int64(time.Millisecond)
			if currTime-finishTime > TIMEOUT_MS {
				delete(completedRequests, requestID)
			}
		}
	}
}

func serverHandlePut(membership *Membership, localFiles *LocalFiles, request *Request) (bool) {
	hostname, _ := os.Hostname()
	fileGroup, contains := localFiles.Files[request.FileName]
	
	// Only thing we do for the put when the node is the fileMaster is send the info to leader
	if contains && fileGroup[0] == hostName { 
		pendingResponse := PendingResponse{
			RequestID: request.RequestID,
			Timestamp: localFiles.UpdateTimes[request.FileName],
			NodeList: fileGroup,
		}
		
		jsonPending, _ := json.Marshal(pendingResponse)
		nodeMessage := server.NodeMessage{
			MsgType: "PendingPut",
			FileName: "",
			SrcHost: "",
			Data: jsonPending,
		}

		leaderHostName := membership.List[0]
		contactNode(leaderHostName, nodeMessage)
		return true
	}

	return contains
}

func serverHandleGet(membership *Membership, localFiles *LocalFiles, request *Request) (bool) {
	hostname, _ := os.Hostname()
	fileGroup, contains := localFiles.Files[request.FileName]

	// If the current node contains the file and the node is the file master, send the file
	if contains && fileGroup[0] == hostName {
		pendingResponse := PendingResponse{
			RequestID: request.RequestID,
			Timestamp: 0,
			NodeList: fileGroup,
		}
		
		jsonPending, _ := json.Marshal(pendingResponse)
		nodeMessage := server.NodeMessage{
			MsgType: "PendingGet",
			FileName: "",
			SrcHost: "",
			Data: jsonPending,
		}
		
		leaderHostName := membership.List[0]
		contactNode(leaderHostName, nodeMessage)
		return true
	}

	return contains
}

func serverHandleDelete(membership *Membership, localFiles *LocalFiles, request *Request) (bool) {
	if _, contains := localFiles.Files[request.FileName]; contains {
		delete(localFiles.Files, request.FileName)
		delete(localFiles.UpdateTimes, request.FileName)

		err := os.Remove("../" + PARENT_DIR + "/" + request.FileName)
		if err != nil {
			log.Infof("Unable to remove file %s from local node!", request.FileName)
			return false
		}

		pendingResponse := PendingResponse{
			RequestID: request.RequestID,
			Timestamp: 0,
			NodeList: []string,
		}
		
		jsonPending, _ := json.Marshal(pendingResponse)
		nodeMessage := server.NodeMessage{
			MsgType: "PendingDelete",
			FileName: "",
			SrcHost: "",
			Data: jsonPending,
		}
		
		leaderHostName := membership.List[0]
		contactNode(leaderHostName, nodeMessage)
		return true
	}
	
	return false
}

func serverHandleLs(membership *Membership, localFiles *LocalFiles, request *Request) (bool) {
	hostname, _ := os.Hostname()
	fileGroup, contains := fileInfo[request.FileName]

	// If the current node contains the file and the node is the file master
	if contains && fileGroup[0] == hostName {
		socket := establishTCP(request.SrcHost)
		if socket == nil {
			return false
		}

		// Send the string to the client
		fileGroupString := strings.Join(fileGroup, ",")
		pendingResponse := PendingResponse{
			RequestID: request.RequestID,
			Timestamp: 0,
			NodeList: fileGroupString,
		}

		jsonPending, _ := json.Marshal(pendingResponse)
		nodeMessage := server.NodeMessage{
			MsgType: "PendingLs",
			FileName: "",
			SrcHost: "",
			Data: jsonPending,
		}
		
		leaderHostName := membership.List[0]
		contactNode(leaderHostName, nodeMessage)
		return true
	}

	return contains
}

// Helper method that will contact a specified node and send a node message
func contactNode(nodeHostName string, leaderMessage *NodeMessage) {
	leaderHostName := membership.List[0]
	tcpAddr, err := net.ResolveTCPAddr("tcp", leaderHostName + ":" + NODE_PORT_NUM)
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
func establishTCP(hostname string) (*net.TCPConn) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", hostname + ":" + NODE_PORT_NUM)
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
	currentFiles := map[string]int
	for _, host := range membership.List {
		currentFiles[host] = 0
	}

	// Loop through every node in every file stored in the local system
	for fileName, fileGroup := range LocalFiles.Files {

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
	for i := 0; i < NUM_REPLICAS - len(runningNodes); i++ {
		
		// Disgusting random function that will loop until it finds a node not in the runningNodes list
		randIndex := 0
		for {
			randIndex = math.Intn(len(membership.List))
			nodeName := membership.List[randIndex]
			for i := 0; i < len(runningNodes); i++ {
				if runningNodes[i] == nodeName {
					continue
				}
			}

			break
		}

		loadedFile := ioutil.ReadFile(PARENT_DIR + "/" + fileName)
		nodeMessage := NodeMessage{
			MsgType: "ReshardRequest",
			FileName: file,
			SrcHost: localHost,
			Data: loadedFile,
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
