package server

// This file will handle all communcation with the server
// So this server will

import (
	"encoding/json"
	"io"
	log "github.com/sirupsen/logrus"
	"math/rand"
	"net"
	"os"
	"time"
)

type NodeMessage struct {
	MsgType   string
	FileName  string
	SrcHost   string
	Data      []byte
}

type ClientRequest struct {
	MsgType  string
	FileName string
	SrcHost  string
}

var SERVER_PORT string = "5000"
var CLIENT_PORT string = "6000"
var FILE_PORT string = "7000"
var REQUEST_TIMEOUT int64 = 5000

// goroutine that serverMain will call that will handle accepting TCP connections
// Needs to handle incoming TCP connections from other nodes as well as incoming connections from the client
func TCPManager(membership *Membership, localFiles *LocalFiles) {

	// This will be used to communicate between goroutines
	infoTransfer := map[int]int{}

	go TcpClientListener(membership, infoTransfer)
	go TcpServerListener(membership, infoTransfer)
	go TcpFileListener(localFiles)
}

func openTCPListener(portNum string) (*net.TCPListener) {
	hostname, _ := os.Hostname()
	tcpAddr, err := net.ResolveTCPAddr("tcp", hostname+":"+portNum)
	if err != nil {
		log.Fatal("Could not resolve the hostname!")
	}

	listener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Fatal("Could not start listening at the address!")
	}

	return listener
}

// This will listen for any client connection and will process their request
func TcpClientListener(membership *Membership, infoTransfer map[int]int) {
	listener := openTCPListener(CLIENT_PORT)
	requestID := 1000

	for {
		conn, _ := listener.AcceptTCP()
		buffer := make([]byte, 1024)
		readLen, _ := conn.Read(buffer)

		clientRequest := ClientRequest{}
		json.Unmarshal(buffer[:readLen], &clientRequest)

		request := &Request{
			ID:       requestID,
			Type:     clientRequest.MsgType,
			SrcHost:  clientRequest.SrcHost,
			FileName: clientRequest.FileName,
		}
		requestID++
		go handleRequest(membership, request, infoTransfer)
	}
}

func handleRequest(membership *Membership, request *Request, infoTransfer map[int]int) {
	startTime := time.Now().UnixNano() / int64(time.Millisecond)
	ticker := time.NewTicker(1000 * time.Millisecond)

	for {
		<-ticker.C

		// This means that the server listener recieved info on the requestID
		if _, contains := infoTransfer[request.ID]; contains {
			break
		}

		currTime := time.Now().UnixNano() / int64(time.Millisecond)
		if currTime-startTime > REQUEST_TIMEOUT {
			tcpAddr, _ := net.ResolveTCPAddr("tcp", request.SrcHost+":"+FILE_PORT)
			socket, _ := net.DialTCP("tcp", nil, tcpAddr)
			
			// Special case where timeout is different for the get request
			if request.Type == "put" {
				randomNodes := getRandomNodes(membership)
				jsonResponse, _ := json.Marshal(randomNodes)
				socket.Write(jsonResponse)
			} else {
				socket.Write([]byte("fail"))
			}
			break
		}
	}

	// Remove the request from the pending request bus	
	for i := 0; i < len(membership.Pending); i++ {
		if membership.Pending[i].ID == request.ID {
			membership.Pending = append(membership.Pending[:i], membership.Pending[i+1:]...)
			membership.LeaderUpdateTime = time.Now().UnixNano() / int64(time.Millisecond)
			return
		}
	}

	// Cleanup info transfer so it doesnt get too big
	delete(infoTransfer, request.ID)
}

// Stupid unique random number generator
func getRandomNodes(membership *Membership) ([]string) {
	nodeList := []string{}
	randNums := map[int]int{}
	for {
		randIndex := rand.Intn(len(membership.List))
		if _, contains := randNums[randIndex]; !contains {
			randNums[randIndex] = 0
			nodeList = append(nodeList, membership.List[randIndex])
		}
		
		if len(nodeList) == 4  || len(nodeList) == len(membership.List) {
			break
		}
	}

	return nodeList
}

func TcpServerListener(membership *Membership, infoTransfer map[int]int) {
	listener := openTCPListener(SERVER_PORT)

	for {
		conn, _ := listener.AcceptTCP()
		buffer := make([]byte, 1024)
		readLen, _ := conn.Read(buffer)
	
		nodeMessage := &NodeMessage{}
		json.Unmarshal(buffer[:readLen], &nodeMessage)

		if nodeMessage.MsgType == "PendingPut" {
			// We need to send the list back to the client
		}

		if nodeMessage.MsgType == "PendingPut" ||
		   nodeMessage.MsgType == "PendingGet" ||
		   nodeMessage.MsgType == "PendingDelete" ||
		   nodeMessage.MsgType == "PendingLs" {
			pendingResponse := &PendingResponse{}
			json.Unmarshal(nodeMessage.Data, pendingResponse)
		
			// We will do different thing later
			infoTransfer[pendingResponse.ID] = 0
		}
	}
}

func TcpFileListener(localFiles *LocalFiles) {
	listener := openTCPListener(FILE_PORT)

	for {
		// We need to somehow populate the fileSystem with the other nodes with the same file
		conn, _ := listener.AcceptTCP()
		buffer := make([]byte, 1024)
		readLen, _ := conn.Read(buffer)

		var fileName string	
		json.Unmarshal(buffer[:readLen], &fileName)

		localFilePath := "../" + PARENT_DIR + "/" + fileName
		fileDes, err := os.Open(localFilePath)
		if err != nil {
			log.Infof("Unable to open local file: %s", err)
			continue
		}

		io.Copy(fileDes, conn)
		fileDes.Close()
	}
}
