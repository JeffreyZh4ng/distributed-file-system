package server

// This file will handle all communcation with the server
// So this server will

import (
	"os"
	"net"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"time"
	"strings"
)

var SERVER_PORT string = "5000"
var CLIENT_PORT string = "6000" 
var SERVER_PORT string = "7000"
var REQUEST_TIMEOUT int64 = 5000

// goroutine that serverMain will call that will handle accepting TCP connections
// Needs to handle incoming TCP connections from other nodes as well as incoming connections from the client
func RequestListener(membership *Membership, localfiles *LocalFiles) {
	
	// This will be used to communicate between goroutines
	infoTransfer = map[int][]string

	go TcpClientListener(membership, infoTransfer)
	go TcpServerListener(membership, localFiles, infoTransfer)
}

func openTCPListener(portNum string) {
	hostname, _ := os.Hostname()	
	tcpAddr, err := ResolveTCPAddr("tcp", hostname + ":" + portNum)
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
func TcpClientListener(membership *Membership, infoTransfer map[int][]string) {
	listener = openTCPListener(CLIENT_PORT)
	requestID := 1000
	
	for {
		conn, _ := listener.AcceptTCP()
		readLen, buffer := make(byte[], 1024)
		conn.Read(msgbuf)

		clientRequest := ClientRequest{}
		json.Unmarshal(msgbuf[:readLen], &clientRequest)

		request := &Request{
			ID: requestID,
			Type: clientRequest.RequestType,
			SrcHost: clientRequest.SrcHost
			FileName: clientRequest.FileName,
		}
		requestID++
		
		switch clientRequest.RequestType {
		case "get":
			go handleGetRequest(membership, request)
		case "put":
			go handlePutRequest(membership, request, infoTransfer)
		case "delete":
			go handleDeleteRequest(membership, request)
		case "ls":
			go handleLsRequest(membership, request)
		}	
	}
}

func handleGetRequest(membership *Membership, request *Request) {
	
	// Wait three seconds before removing the request from the pending list
	timer := time.NewTimer(3 * time.Second)
	<-timer.C

	for i := 0; i < len(membership.Pending); i++ {
		if membership.Pending[i].ID == request.ID {
			membership.Pending = append(membership.Pending[:i], membership.Pending[i+1:]...)
			return
		}
	}
}

func handlePutRequest(membership *Membership, request *Request) {
	startTime := time.Now().UnixNano() / int64(time.Millisecond)
	elapsedTime := 0
	ticker := time.NewTicker(1000 * time.Millisecond)
	
	for {
		<-ticket.C

		if _, contains := infoTransfer[requestID]; contains {
			break
		}

		currTime := time.Now().UnixNano() / int64(time.Millisecond)
		elapsedTime += (currTime - startTime)
		if elapsedTime > REQUEST_TIMEOUT {
			log.Info("Server timed out waiting for get request, file does not exist!")
			return
		} 
	}
}

func TcpServerListener(membership *Membership, localfiles *LocalFiles) {
	// listener = openTCPListener(SERVER_PORT)
	
	for {
		// conn, _ := listener.AcceptTCP()
		// readLen, buffer := make(byte[], 1024)
		// json.Unmarshal(msgbuf[:readLen], &clientRequest)

	}
}
