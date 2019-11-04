package server

// This file will handle all communcation with the server
// So this server will

type NodeMessage struct {
	MsgType   string,
	FileName  string,
	SrcHost   string,
	Data      []byte,
}

// goroutine that serverMain will call that will handle accepting TCP connections
// Needs to handle incoming TCP connections from other nodes as well as incoming connections from the client
func TcpServerListener() {
}

// This function should be passed in the membership and filesystem and it will add a request to the Pending Requests list based on the TCP connection it recieved
func processTCPConnection()
