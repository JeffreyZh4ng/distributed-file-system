package client

import (
	log "github.com/sirupsen/logrus"
	"cs-425-mp3/rpcParams"
)

// Each time the client makes a request, it sends it to all possible servers
// However, only the leader will process the request
func dialServers() {

	// Do this in a goroutine. The goroutine that returns has the response from the leader
	client, err := rpc.DialHTTP("tcp", ":1234")
	if err != nil {
		// This means that the server wasnt on
	}

	args := &rpcexample.Args{
		A: 2,
		B: 3,
	}

	//this will store returned result
	var result rpcexample.Result
	//call remote procedure with args

	err = client.Call("Arith.Multiply", args, &result)
	if err != nil {
		log.Fatalf("error in Arith", err)
	}
}

func PutRequest(args []string) {
}

func GetRequest(args []string) {
}

func DeleteRequest(args []string) {
}

func LsCommand(args []string) {
}
