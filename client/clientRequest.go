package client

import (
	log "github.com/sirupsen/logrus"
	"os"
	"net"
	"io"
	"encoding/json"
	"sync"
	"strconv"
)

var CLIENT_MSG_PORT string = "23333" // For message 
var FILE_TRANSFER_PROT string = "43991" // For file
var FILE_ROOT string = "/home/zx27/SDFS/"

type FileMsg struct{
	MsgType string
	FileName string
	Data []string
	SrcHost string
}

func initRequest(requestType string, fileName string) *net.TCPConn{
	localHost, _ := os.Hostname()

	for i := 1; i <= 10; i++ {
		numStr := strconv.Itoa(i)
		connectName := "fa19-cs425-g84-0" + numStr + ".cs.illinois.edu"

		clientRequestConn, err := establishTCP(connectName, CLIENT_MSG_PORT)
		if err != nil{
			log.Infof("Could not connect to %s, try next", connectName)
			continue
		}
		lsmsg := FileMsg{
			MsgType: requestType,
			FileName: fileName,
			Data: []string{},
			SrcHost: localHost,
		}
		lsmsgbt, err := json.Marshal(lsmsg)
		clientRequestConn.Write(lsmsgbt)

		return clientRequestConn
	}
	return nil
}
func ClientDel(args []string){
	fileName := args[0]
	clientRequestConn := initRequest("ls_init", fileName)
	clientRequestConn.Close()
}

func ClientLs(args []string){
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
		newFileMsg := FileMsg{}
		err = json.Unmarshal(msgbuf[:msglen], &newFileMsg)
		if err != nil {
			log.Infof("Unable to decode put respond msg %s", err)
			return
		}
		log.Info(newFileMsg.Data)
		clientRequestConn.Close()
}

func ClientGet(args []string){
	fileName := args[0]
	clientRequestConn := initRequest("ls_init", fileName)
	localFilePath := FILE_ROOT + args[1]
	 
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
	if newFileMsg.MsgType == "get_ok"{ // fetch file from the source 
		fConn, err := establishTCP(newFileMsg.Data[0], FILE_TRANSFER_PROT)
		if err != nil{
			log.Infof("Could not connect to file source, %s", err)
			return
		}
		TCPFileReceiver(localFilePath, fConn)
		fConn.Close()
	}else{
		log.Infof("The file is unavailable")
		return
	}
	clientRequestConn.Close()


}

func ClientPut(args []string){
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

// For put and ls
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
	clientRequestConn, err := establishTCP(connectName, CLIENT_MSG_PORT)
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
	tcpAddr, err := net.ResolveTCPAddr("tcp", ":"+FILE_TRANSFER_PROT)
	flistener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Infof("Server could not set up TCP listener %s", err)
		return
	}
	log.Infof("Listen on %s!", FILE_TRANSFER_PROT)

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

func TCPFileReceiver(filePath string, readConn *net.TCPConn){
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