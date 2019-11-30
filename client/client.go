package client

import (
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"cs-425-mp3/server"
	"io"
	"net"
	"os"
	"strconv"
)

var PARENT_DIR string = "nodeFiles"

// Function that will try to dial the lowest number server possible
func initClientRequest(requestType string, args *server.RequestArgs) (*server.ResponseArgs) {
	for i := 1; i <= 10; i++ {
		numStr := strconv.Itoa(i)
		connectName := "fa19-cs425-g84-" + numStr + ".cs.illinois.edu"
		response := dialServer(connectName, requestType, args)

		if response != nil {
			log.Infof("Connected to server: %s", connectName)
			return response
		}
	} 

	log.Fatal("Could not connect to any server!")
	return nil
}

// Function that dials a specific server
func dialServer(hostname string, requestType string, args *server.RequestArgs) (*server.ResponseArgs) {
	client, err := rpc.DialHTTP("tcp", hostname + ":" + SERVER_PORT_NUM)
	if err != nil {
		return nil
	}

	var response handleRPC.ResponseArgs
	err = client.Call(requestType, args, &response)
	if err != nil {
		log.Fatalf("error in Request", err)
		return nil
	}

	return &response
}


func initRequest(requestType string, fileName string) {
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

		jsonRequest, _ := json.Marshal(clientRequest)
		socket.Write(jsonRequest)
		return
	}
	
	log.Fatal("Server could not reach any server!")
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
	hostname, _ := os.Hostname()
	fileName := args[0]
	initRequest("get", fileName)

	tcpAddr, _ := net.ResolveTCPAddr("tcp", hostname+":"+FILE_PORT)
	socket, _ := net.ListenTCP("tcp", tcpAddr)
	readConn, _ := socket.AcceptTCP()
	defer socket.Close()

	buffer := make([]byte, 4)
	readConn.Read(buffer)
	if string(buffer) == "fail" {
		log.Infof("The file %s was not found in the system!", fileName)
		return
	}
	
	filePath := PARENT_DIR + "/" + args[1]
	writeFile(filePath, readConn)
}

func ClientPut(args []string) {
	hostname, _ := os.Hostname()
	fileName := args[1]
	initRequest("put", fileName)
	
	tcpAddr, _ := net.ResolveTCPAddr("tcp", hostname+":"+CLIENT_PORT)
	socket, _ := net.ListenTCP("tcp", tcpAddr)
	readConn, _ := socket.AcceptTCP()
	defer socket.Close()

	buffer := make([]byte, 1024)
	readlen, _ := readconn.read(buffer)

	var putnodes []string
	json.unmarshal(buffer[:readlen], &putnodes)

	log.Infof("Nodes that the client is writing to %s", putNodes)	
	localFilePath := PARENT_DIR + "/" + args[0]
	file, err := os.Open(localFilePath)
	if err != nil {
		log.Infof("Unable to open local file: %s", err)
		return
	}
	
	for i := 0; i < len(putNodes); i++ {
		socket := establishTCP(putNodes[i], FILE_PORT)
		jsonName, _ := json.Marshal(fileName)
		socket.Write(jsonName)
		
		file.Seek(0, io.SeekStart)
		_, err = io.Copy(socket, file)
		log.Infof("Client wrote to node %s", putNodes[i])
	}
}

func ClientDel(args []string) {
	fileName := args[0]
	initRequest("delete", fileName)

	tcpAddr, _ := net.ResolveTCPAddr("tcp", hostname+":"+CLIENT_PORT)
	socket, _ := net.ListenTCP("tcp", tcpAddr)
	readConn, _ := socket.AcceptTCP()
	defer socket.Close()

	buffer := make([]byte, 4)
	readConn.Read(buffer)
	if string(buffer) == "fail" {
		log.Infof("The file %s was not found in the system!", fileName)
		return
	}

	log.Infof("%d was deleted from the system!", fileName)
}

func ClientLs(args []string) {
	fileName := args[0]
	initRequest("ls", fileName)

	tcpAddr, _ := net.ResolveTCPAddr("tcp", hostname+":"+CLIENT_PORT)
	socket, _ := net.ListenTCP("tcp", tcpAddr)
	readConn, _ := socket.AcceptTCP()
	defer socket.Close()

	buffer := make([]byte, 4)
	readConn.Read(buffer)
	if string(buffer) == "fail" {
		log.Infof("The file %s was not found in the system!", fileName)
		return
	}

	buffer = make([]byte, 1024)
	readlen, _ := readconn.read(buffer)

	var hosts []string
	json.unmarshal(buffer[:readlen], &hosts)
	log.Infof("Nodes that contain file %s:\n%s", fileName, hosts)
}

func writeFile(filePath string, readConn *net.TCPConn) {
	fileDes, err := os.OpenFile(filePath, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Infof("Unable to create local file: %s", err)
		return
	}
	io.TeeReader(fileDes, readConn)
	fileDes.Close()
}