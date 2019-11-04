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

type NodeMessage struct {
	MsgType   string,
	FileName  string,
	SrcHost   string,
	Data      []byte,
}

// goroutine that serverMain will call that will handle accepting TCP connections
// Needs to handle incoming TCP connections from other nodes as well as incoming connections from the client
func TcpServerListener(l *net.TCPListener, membership *Membership, localfiles *LocalFiles) {
	for{
		conn, err := l.AcceptTCP()
		if err != nil {
			log.Fatal("TCP listener accept error ", err)
		}
		go processTCPConnection(conn, membership, localfiles)
	}
}

// This function should be passed in the membership and filesystem and it will add a request to the Pending Requests list based on the TCP connection it recieved
func processTCPConnection(conn *net.TCPConn, membership *Membership, localfiles *LocalFiles){
	msgbuf := make([]byte, 1024)
	msglen, err := conn.Read(msgbuf)
	if err != nil {
		log.Infof("TCP read error %s", err)
		return
	}	
	newMsg := NodeMessage{}
	err = json.Unmarshal(msgbuf[:msglen], &newMsg)
	if err != nil {
		log.Infof("Unable to decode put respond msg %s", err)
		return
	}
	if strings.Contains(NodeMessage.MsgType, "init"){
		// It's request from client 
	} 

}

func processClientRequest(clientConn *net.TCPConn, clientMsg NodeMessage membership *Membership, localfiles *LocalFiles, requestPool map[int]bool){

}
