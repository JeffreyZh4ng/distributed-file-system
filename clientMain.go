package main

import (
	log "github.com/sirupsen/logrus"
	"cs-425-mp3/client"
	"os"

)

func parseArgs() (string, []string) {
	if len(os.Args) < 3 {
		log.Fatal("User did not specify enough arguments")
	}
	command := os.Args[1]
	args := os.Args[2:]
	
	return command, args
}

func main() {
	command, args := parseArgs()
	if command == "put" && len(args) == 2 {
		client.ClientPut(args)
	} else if command == "get" && len(args) == 2 {
		client.ClientGet(args)
	} else if command == "delete" && len(args) == 1 {
		client.ClientDel(args)
	} else if command == "ls" && len(args) == 1 {
		client.ClientLs(args)
	} else {
		log.Fatal("Command not recognized or invalid args")
	}
}
