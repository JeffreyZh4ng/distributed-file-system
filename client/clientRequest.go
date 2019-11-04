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

var CLIENT_MSG_PORT string = "6000"    // For message
var FILE_TRANSFER_PORT string = "7000" // For file
var FILE_ROOT string = "/home/zx27/SDFS/"

type NodeMessage struct {
	MsgType  string
	FileName string
	SrcHost  string
	Data     []byte
}
type ClientRequest struct {
	MsgType  string
	FileName string
	SrcHost  string
}

func initRequest(requestType string, fileName string) *net.TCPConn {
	localHost, _ := os.Hostname()

	for i := 1; i <= 10; i++ {
		numStr := strconv.Itoa(i)
		if len(numStr) == 1 {
			numStr = "0" + numStr
		}
		connectName := "fa19-cs425-g84-" + numStr + ".cs.illinois.edu"

		clientRequestConn, err := establishTCP(connectName, CLIENT_MSG_PORT)
		if err != nil {
			log.Infof("Could not connect to %s, try next", connectName)
			continue
		}
		lsmsg := NodeMessage{
			MsgType:  requestType,
			FileName: fileName,
			SrcHost:  localHost,
			Data:     []byte{},
		}
		lsmsgbt, err := json.Marshal(lsmsg)
		clientRequestConn.Write(lsmsgbt)

		log.Info("TEST1")
		return clientRequestConn
	}
	return nil
}

func ClientDel(args []string) {
	fileName := args[0]
	clientRequestConn := initRequest("ls_init", fileName)
	clientRequestConn.Close()
}

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
}

func ClientGet(args []string) {
	fileName := args[0]

	clientRequestConn := initRequest("ls_init", fileName)
	localFilePath := FILE_ROOT + args[1]
	waitingforFile(localFilePath)
	log.Info("TEST")

	msgbuf := make([]byte, 1024)
	clientRequestConn.SetReadDeadline(time.Now().Add(time.Second * 5))
	msglen, err := clientRequestConn.Read(msgbuf)
	if err != nil {
		log.Infof("File is not available")
		return
	}
	newNodeMessage := NodeMessage{}
	err = json.Unmarshal(msgbuf[:msglen], &newNodeMessage)
	if err != nil {
		log.Infof("Unable to decode put respond msg %s", err)
		return
	}
	defer clientRequestConn.Close()
}
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

}

// For put and ls

func handleLeaderResponse(responseMsg NodeMessage, fileName string, localHost string, localFilePath string) {
	requestMsgTofileMaster := NodeMessage{}
	switch responseMsg.MsgType {
	case "insert_confirm": // There is no file in SDFD now
		requestMsgTofileMaster = NodeMessage{
			MsgType:  "put",
			FileName: fileName,
			Data:     responseMsg.Data,
			SrcHost:  localHost,
		}
	case "update_confirm": // Client will send update request to fileMaster
		requestMsgTofileMaster = NodeMessage{
			MsgType:  "put",
			FileName: fileName,
			Data:     responseMsg.Data,
			SrcHost:  localHost,
		}
	default:
		log.Infof("No reply from leader, fail to put")
		return
	}
	var connectName string
	if len(responseMsg.Data) > 0 {
		connectName = string(responseMsg.Data)
	} else {
		log.Infof("No node to contact")
		return
	}
	clientRequestConn, err := establishTCP(connectName, CLIENT_MSG_PORT)
	if err != nil {
		log.Infof("Could not connect to %s, fail to put", connectName)
		return
	}
	putToMaster, err := json.Marshal(requestMsgTofileMaster)
	clientRequestConn.Write(putToMaster)

	//Waiting for file master reply
	msgbuf := make([]byte, 1024)
	msglen, err := clientRequestConn.Read(msgbuf)
	if err != nil {
		log.Infof("TCP read error %s", err)
		return
	}
	newNodeMessage := NodeMessage{}
	err = json.Unmarshal(msgbuf[:msglen], &newNodeMessage)
	if err != nil {
		log.Infof("Decode error from master %s", err)
		return
	}
	if newNodeMessage.MsgType == "put_prompt_ok" {
		log.Info(newNodeMessage.Data)

		var wg sync.WaitGroup
		wg.Add(1)
		go establishTCPFileServer(&wg, localFilePath, len(newNodeMessage.Data))
		confirmMsg := NodeMessage{
			MsgType:  "put_confirmation",
			FileName: fileName,
			Data:     []Data{},
			SrcHost:  localHost,
		}
		putconbt, _ := json.Marshal(confirmMsg)
		clientRequestConn.Write(putconbt)
		clientRequestConn.Close()
		wg.Wait()
	} else {
		log.Infof("Write Confilict!")
		return
	}

}
*/

// Helper that establishes a TCP connection
func establishTCP(hostname string, portNumber string) (*net.TCPConn, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", hostname+":"+portNumber)
	if err != nil {
		log.Info("Could not resolve the hostname!")
		return nil, err
	}

	socket, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		log.Info("Server could not dial the client!")
		return nil, err
	}

	return socket, err
}

// Concurrency sending:
func establishTCPFileServer(wg *sync.WaitGroup, filePath string, numReplicas int) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", ":"+FILE_TRANSFER_PORT)
	flistener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Infof("Server could not set up TCP listener %s", err)
		return
	}
	log.Infof("Listen on %s!", FILE_TRANSFER_PORT)

	for i := 0; i < numReplicas; i++ {
		rdfile, err := os.Open(filePath)
		if err != nil {
			log.Infof("Unable to open local file: %s", err)
			return
		}
		destConn, err := flistener.AcceptTCP()
		if err != nil {
			log.Infof("Unable resolve file destination: %s", err)
			return
		}
		io.Copy(destConn, rdfile)
		destConn.Close()
		rdfile.Close()
	}
	wg.Done()
}

func TCPFileReceiver(filePath string, readConn *net.TCPConn) {
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

func waitingforFile(filePath string) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", ":"+FILE_TRANSFER_PORT)
	log.Infof("0%s", err)
	l, err := net.ListenTCP("tcp", tcpAddr)
	log.Infof("1%s", err)
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
