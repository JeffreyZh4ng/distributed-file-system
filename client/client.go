package client

import (
	log "github.com/sirupsen/logrus"
	"cs-425-mp3/server"
	"io/ioutil"
	"math/rand"
	"net/rpc"
	"strconv"
)

var CLIENT_FOLDER_NAME string = "clientFiles/"

// Function that will try to dial the lowest number server possible
func initClientRequest(requestType string, fileName string) (*server.ClientResponseArgs) {
	for i := 1; i <= 10; i++ {
		numStr := strconv.Itoa(i)
		if len(numStr) == 1 {
			numStr = "0" + numStr
		}

		connectName := "fa19-cs425-g84-" + numStr + ".cs.illinois.edu"
		response := makeClientRequest(connectName, requestType, fileName)

		if response != nil {
			log.Infof("Connected to server %s and recieved a response", connectName)
			return response
		}
	} 

	log.Fatal("Could not connect to any server!")
	return nil
}

// Function that dials a specific server
func makeClientRequest(hostname string, requestType string, fileName string) (*server.ClientResponseArgs) {
	log.Infof("Making client request to %s", hostname + ":" + server.CLIENT_RPC_PORT)
	client, err := rpc.DialHTTP("tcp", hostname + ":" + server.CLIENT_RPC_PORT)
	if err != nil {
		return nil
	}
	defer client.Close()

	var response server.ClientResponseArgs
	err = client.Call(requestType, fileName, &response)
	if err != nil {
		log.Fatalf("error in Request", err)
		return nil
	}

	return &response
}

// Function that will dial a server to request or send a file
func makeFileTransferRequest(hostname string, requestType string, request *server.FileTransferRequest) ([]byte) {
	log.Infof("Making transfer request to %s", hostname + ":" + server.CLIENT_RPC_PORT)
	client, err := rpc.DialHTTP("tcp", hostname + ":" + server.FILE_RPC_PORT)
	if err != nil {
		return nil
	}
	defer client.Close()
		
	var response server.TransferResult
	err = client.Call(requestType, &request, &response)
	if err != nil {
		log.Fatalf("error in Request", err)
		return nil
	}
		
	return response
}

func ClientPut(args []string) {
	fileName := args[1]
	response := initClientRequest("ClientRequest.Put", fileName)
	log.Infof("Putting file to %s", response.HostList)

	filePath := CLIENT_FOLDER_NAME + args[0]
	fileContents, _ := ioutil.ReadFile(filePath)

	request := &server.FileTransferRequest{
		FileName: fileName,
		FileGroup: response.HostList,
		Data: fileContents,
	}

	for i := 0; i < len(response.HostList); i++ {
		makeFileTransferRequest(response.HostList[i], "FileTransfer.SendFile", request)
	}
}

func ClientGet(args []string) {
	fileName := args[0]
	response := initClientRequest("ClientRequest.Get", fileName)

	if !response.Success {
		log.Infof("File %s not found in the sdfs!", fileName)
	}

	requestServer := response.HostList[rand.Intn(len(response.HostList))]
	request := &server.FileTransferRequest{
		FileName: fileName,
		FileGroup: nil,
		Data: nil,
	}
	transferResponse := makeFileTransferRequest(requestServer, "FileTransfer.GetFile", request)

	filePath := CLIENT_FOLDER_NAME + args[1]
	err := ioutil.WriteFile(filePath, transferResponse, 0666)
	if err != nil {
		log.Fatalf("Unable to write bytes from get!", err)
	}
}

func ClientDel(args []string) {
	fileName := args[0]
	response := initClientRequest("ClientRequest.Delete", fileName)

	if response.Success {
		log.Infof("File %s deleted from the sdfs!", fileName)
	} else {
		log.Infof("File %s not found in the sdfs!", fileName)
	}
}

func ClientLs(args []string) {
	fileName := args[0]
	response := initClientRequest("ClientRequest.List", fileName)

	if response.Success {
		log.Infof("File %s is stored at:\n%s", response.HostList)
	} else {
		log.Infof("File %s not found in the sdfs!", fileName)
	}
}