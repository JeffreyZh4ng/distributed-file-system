package client

import (
	
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

var CLIENT_PORT string = "6000"
var FILE_PORT string = "7000"
var PARENT_DIR string = "nodeFiles"

type ClientRequest struct {
	MsgType  string
	FileName string
	SrcHost  string
}

func initRequest(requestType string, fileName string) (*net.TCPConn) {
	localHost, _ := os.Hostname()

	for i := 1; i <= 10; i++ {
		numStr := strconv.Itoa(i)
		if len(numStr) == 1 {
			numStr = "0" + numStr
		}
		connectName := "fa19-cs425-g84-" + numStr + ".cs.illinois.edu"

		socket := establishTCP(connectName, CLIENT_PORT)
		if socket == nil {
			log.Infof("Could not connect to %s", connectName)
			continue
		}

		clientRequest := ClientRequest{
			MsgType:  requestType,
			FileName: fileName,
			SrcHost:  localHost,
		}

		jsonRequest, err := json.Marshal(clientRequest)
		socket.Write(jsonRequest)
		return socket
	}

	return nil
}

// Helper that establishes a TCP connection
func establishTCP(hostname string, portNumber string) (*net.TCPConn) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", hostname+":"+portNumber)
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

func ClientGet(args []string) {
	hostname, _. := os.Hostname()
	fileName := args[0]
	initRequest("get", fileName)

	tcpAddr, _ := net.ResolveTCPAddr("tcp", hostname+":"+FILE_PORT)
	socket, _ := net.ListenTCP("tcp", tcpAddr)
	readConn, _ := socket.AcceptTCP()
	defer socket.Close()

	buffer := make([]byte, 4)
	readLen, _ := readConn.Read(buffer)
	if string(buffer) == "fail" {
		log.Infof("The file %f was not found in the system!", fileName)
		return
	}
	
	filePath := "../" + PARENT_DIR + "/" + args[1]
	writeFile(filePath, readConn)
}

/*
func ClientDel(args []string) {
	fileName := args[0]
	clientRequestConn := initRequest("ls_init", fileName)
	clientRequestConn.Close()
}
*/

/*
func ClientLs(args []string) {
	fileName := args[0]
	clientRequestConn := initRequest("ls_init", fileName)
	// Make ls request message sent to leader

	// Waiting for leader response
	msgbuf := make([]byte, 1024)
	msglen, err := clientRequestConn.Read(msgbuf)
	if err != nil {
		log.Infof("TCP read error %s", err)
		return
	}
	newNodeMessage := NodeMessage{}
	err = json.Unmarshal(msgbuf[:msglen], &newNodeMessage)
	if err != nil {
		log.Infof("Unable to decode put respond msg %s", err)
		return
	}
	log.Info(newNodeMessage.Data)
	clientRequestConn.Close()
}*/


/*
func ClientPut(args []string) {
	fileName := args[1]
	clientRequestConn := initRequest("ls_init", fileName)
	localHost, _ := os.Hostname()
	localFilePath := FILE_ROOT + args[0]

	// Waiting for leader response
	msgbuf := make([]byte, 1024)
	msglen, err := clientRequestConn.Read(msgbuf)
	if err != nil {
		log.Infof("TCP read error %s", err)
		return
	}
	newNodeMessage := NodeMessage{}
	err = json.Unmarshal(msgbuf[:msglen], &newNodeMessage)
	if err != nil {
		log.Infof("Unable to decode put respond msg %s", err)
		return
	}
	log.Info(newNodeMessage.Data)
	clientRequestConn.Close()
	handleLeaderResponse(newNodeMessage, fileName, localHost, localFilePath)
	return

}*/



func writeFile(filePath string, readConn *net.TCPConn) {
	fileDes, err := os.Create(filePath)
	if err != nil {
		log.Infof("Unable to create local file: %s", err)
		return
	}
	io.Copy(fileDes, readConn)
	mkfile.Close()
}

func waitingforFile(filePath string) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", ":"+FILE_TRANSFER_PORT)
	l, err := net.ListenTCP("tcp", tcpAddr)
	startTime := time.Now().UnixNano() / int64(time.Millisecond)
	
	for {
		curTime := time.Now().UnixNano() / int64(time.Millisecond)
		if curTime-startTime > 5000 {
			log.Infof("File unavailable")
			break
		}
		readConn, err := l.AcceptTCP()

		mkfile, err := os.Create(filePath)
		if err != nil {
			log.Infof("Unable to create local file: %s", err)
			return
		}
		io.Copy(mkfile, readConn)
		if err != nil {
			log.Infof("Unable to create local file: %s", err)
			return
		}
		mkfile.Close()
	}
}
