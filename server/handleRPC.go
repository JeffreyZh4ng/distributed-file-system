package server

// Holds arguments to be passed to service request in RPC call
type RequestArgs struct {
        SourceHost string
        Request    string
	Data       []byte

}

// Hold arguments to be returned to the client
type ResponseArgs struct {
        SourceHost string
        Hosts   []string
        Data    []byte
}

// Represents service Request
type RPCRequest int

// Result of RPC call is of this type
type Response ResponseArgs

func (t *RPCRequest) Put(request RequestArgs, response *ResponseArgs) error {
	// This function will look for the file in the system. If it doesnt exist,
	// The leader will return the nodes to write to and if the user need confirmation
	// The client will then RPC connect to the nodes and write the file the client
	// Will handle the confirmation
	return nil
}

func (t *RPCRequest) Get(request RequestArgs, response *ResponseArgs) error {
	// This function will return a response of which node has the specified file
	// The client will then connect to that server with RPC to retrieve the file
	return nil
}

func (t *RPCRequest) Delete(request RequestArgs, response *ResponseArgs) error {
	// This will call a function that will add the request into the queue then it will wait
	// For a response. After a timeout, the function will return with the response of the request
	return nil
}

func (t *RPCRequest) List(request RequestArgs, response *ResponseArgs) error {
	// This function will get a list of all the files from a server
	// It will return if the file was found
	return nil
}
