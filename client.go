package main

import (
	log "github.com/sirupsen/logrus"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

func parseArgs() (string, []string) {
	if len(os.Args) < 3 {
		log.Fatal("User did not specify enough arguments")
	}

	command := os.Args[1]
	args := os.Args[2:]
	
	return command, args
}

func dialServers() {
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

func main() {
	command, args := parseArgs()
	if command == "put" && len(args) == 2 {
		// 
	} else if command == "get" && len(args) == 2 {
		
	} else if command == "delete" && len(args) == 1 {

	} else if command == "ls" && len(args) == 1 {

	} else {
		log.Fatal("Command not recognized or invalid args")
	}
}
