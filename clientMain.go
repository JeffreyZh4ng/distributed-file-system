package main

import (
	log "github.com/sirupsen/logrus"
	"cs-425-mp3/client"
	"os"
	"net"
	"io"
	"encoding/json"
	"sync"
	"strconv"
)

var PORT_NUM_FILE string = "23333" // For message 
var FILE_PORT_NUM string = "43991" // For file
var FILE_ROOT string = "/home/zx27/SDFS/"
type FileMsg struct{
	MsgType string
	FileName string
	Data []string
	SrcHost string
}


func parseArgs() (string, []string) {
	if len(os.Args) < 3 {
		log.Fatal("User did not specify enough arguments")
	}
	command := os.Args[1]
	args := os.Args[2:]
	
	return command, args
}

func clientPut(args []string){
	localHost, _ := os.Hostname()
	fileName := args[1]
	localFilePath := FILE_ROOT + args[0]

	for i := 1; i <= 10; i++ {
		numStr := strconv.Itoa(i)
		connectName := "fa19-cs425-g84-0" + numStr + ".cs.illinois.edu"

		clientRequestConn, err := establishTCP(connectName, PORT_NUM_FILE)
		if err != nil{
			log.Infof("Could not connect to %s, try next", connectName)
			continue
		}
		// Make put request message sent to leader
		putmsg := FileMsg{
			MsgType: "put_init",
			FileName: fileName,
			Data: []string{},
			SrcHost: localHost,
		}
		putmsgbt, err := json.Marshal(putmsg)
		clientRequestConn.Write(putmsgbt)
		
		// Waiting for leader response
		msgbuf := make([]byte, 1024)
		msglen, err := clientRequestConn.Read(msgbuf)
		if err != nil {
			log.Infof("TCP read error %s", err)
			return
		}	
		newFileMsg := FileMsg{}
		err = json.Unmarshal(msgbuf[:msglen], &newFileMsg)
		if err != nil {
			log.Infof("Unable to decode put respond msg %s", err)
			return
		}
		log.Info(newFileMsg.Data)
		clientRequestConn.Close()
		handleLeaderResponse(newFileMsg, fileName, localHost, localFilePath)	
		return
	} 
}

// 
func handleLeaderResponse(responseMsg FileMsg, fileName string, localHost string, localFilePath string){
	requestMsgTofileMaster := FileMsg{}
	switch responseMsg.MsgType{
	case "insert_confirm": // There is no file in SDFD now
		requestMsgTofileMaster = FileMsg{
			MsgType: "put",
			FileName: fileName,
			Data: responseMsg.Data,
			SrcHost: localHost,
		}
	case "update_confirm": // Client will send update request to fileMaster
		requestMsgTofileMaster = FileMsg{
			MsgType: "put",
			FileName: fileName,
			Data: responseMsg.Data,
			SrcHost: localHost,
		}
	default:
		log.Infof("No reply from leader, fail to put")
		return
	}
	var connectName string
	if len(responseMsg.Data) > 0{
		connectName = responseMsg.Data[0]
	}else{
		log.Infof("No node to contact")
		return
	}
	clientRequestConn, err := establishTCP(connectName, PORT_NUM_FILE)
	if err != nil{
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
	newFileMsg := FileMsg{}
	err = json.Unmarshal(msgbuf[:msglen], &newFileMsg)
	if err != nil {
		log.Infof("Decode error from master %s", err)
		return
	}
	if newFileMsg.MsgType == "put_prompt_ok"{
		log.Info(newFileMsg.Data)

		var wg sync.WaitGroup
		wg.Add(1)
		go establishTCPFileServer(&wg, localFilePath, len(newFileMsg.Data))
		confirmMsg := FileMsg{
			MsgType: "put_confirmation",
			FileName: fileName,
			Data: []string{},
			SrcHost: localHost,
		}
		putconbt, _ := json.Marshal(confirmMsg)
		clientRequestConn.Write(putconbt)
		clientRequestConn.Close()
		wg.Wait()
	}else{
		log.Infof("Write Confilict!")
		return
	}

}


// Helper that establishes a TCP connection
func establishTCP(hostname string, portNumber string) (*net.TCPConn, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", hostname + ":" + portNumber)
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
func establishTCPFileServer(wg *sync.WaitGroup, filePath string, numReplicas int){
	tcpAddr, err := net.ResolveTCPAddr("tcp", ":"+FILE_PORT_NUM)
	flistener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Fatal("Server could not set up TCP listener %s", err)
	}
	log.Infof("Listen on %s!", FILE_PORT_NUM)

	for i := 0; i < numReplicas; i++{
		rdfile, err:= os.Open(filePath)
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

func main() {
	command, args := parseArgs()
	if command == "put" && len(args) == 2 {
		clientPut(args)
	} else if command == "get" && len(args) == 2 {
		client.GetRequest(args)
	} else if command == "delete" && len(args) == 1 {
		client.DeleteRequest(args)
	} else if command == "ls" && len(args) == 1 {
		client.LsCommand(args)
	} else {
		log.Fatal("Command not recognized or invalid args")
	}
}