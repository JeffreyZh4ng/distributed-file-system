package server

var SERVER_RPC_PORT string = "6000"

type ServerRequestArgs struct {
	ID string,
	Hostname string,
}

// Represents service Request
type ServerCommunication int

// This RPC call is handled by the leader which will add the server response to the global
// List of server responses (ServerResponses) in fileSystems.go
func (t *ServerCommunication) FileFound(serverResponse *ServerRequestArgs, _) error {
	ServerResponses[serverResponse.ID] = serverResponse.Hostname
	return nil
}
