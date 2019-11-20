package client

import (
	log "github.com/sirupsen/logrus"
	"cs-425-mp3/server"
	"net/rpc"
	"os"
	"strconv"
)

var SERVER_PORT_NUM string = "4000"
var CLIENT_PORT_NUM string = "8000"

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

	var response server.ResponseArgs
	err = client.Call(requestType, args, &response)
	if err != nil {
		log.Fatalf("error in Request", err)
		return nil
	}

	return &response
}

func PutRequest(args []string) {
}

func GetRequest(args []string) {
	hostname, _ := os.Hostname()
	requestArgs := &server.RequestArgs{
		SourceHost: hostname + CLIENT_PORT_NUM,
		Request: "Get",
		Data: []byte(args[0]),
	}	
	
	response := initClientRequest("Request.Get", requestArgs)
	log.Infof("Reponse host: %s", response.SourceHost)
	// Handle the response here
}

func DeleteRequest(args []string) {
}

func LsCommand(args []string) {
}
