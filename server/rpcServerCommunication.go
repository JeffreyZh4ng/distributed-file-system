package server

import (
	"time"
)

var SERVER_RPC_PORT string = "6000"

// We need HostList. Get and List will send over the list of hosts with the file
type ServerRequestArgs struct {
	ID string
	FileName string
	HostList []string
}

// This RPC service handles all communication between servers of the sdfs
type ServerCommunication int

// This RPC call is handled by the leader which will add the server response to the global
// List of server responses (ServerResponses) in fileSystems.go
func (t *ServerCommunication) FileFound(serverResponse *ServerRequestArgs, _ *string) error {
	ServerResponses[serverResponse.ID] = serverResponse.HostList
	return nil
}

// This call will be used to update the fileGroup when file are resharded
func (t *ServerCommunication) UpdateFileGroup(request ServerRequestArgs, _ *string) error {
	LocalFiles.Files[request.FileName] = request.HostList
	LocalFiles.UpdateTimes[request.FileName] = time.Now().UnixNano() / int64(time.Millisecond)

	return nil
}