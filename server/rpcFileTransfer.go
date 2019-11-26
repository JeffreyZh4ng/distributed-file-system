package server

var FILE_RPC_PORT string = "7000"

// Holds arguments to be passed to service request in RPC call
type FileTransferArgs struct {
    FileName string
}

// Hold arguments to be returned to the client
type ResponseArgs struct {
        SourceHost string
        Hosts   []string
        Data    []byte
}

// Represents service Request
type FileTransfer int

func (t *FileTransfer) SendFile(request RequestArgs, response *ResponseArgs) error {
	// This function will look for the file in the system. If it doesnt exist,
	// The leader will return the nodes to write to and if the user need confirmation
	// The client will then RPC connect to the nodes and write the file the client
	// Will handle the confirmation
	return nil
}

func (t *FileTransfer) GetFile(request RequestArgs, response *ResponseArgs) error {
	// This function will look for the file in the system. If it doesnt exist,
	// The leader will return the nodes to write to and if the user need confirmation
	// The client will then RPC connect to the nodes and write the file the client
	// Will handle the confirmation
	return nil
}
