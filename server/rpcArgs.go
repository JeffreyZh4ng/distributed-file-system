package server

//Holds arguments to be passed to service Arith in RPC call
type RequestArgs struct {
        SourceHost string
        Data    []byte
        DataLen int
}

type ResponseArgs struct {
        SourceHost string
        Hosts   []string
        Data    []byte
        DataLen int
}

//Represents service Request
type Request int

//Result of RPC call is of this type
type Response ResponseArgs

func (t *Request) Put(request RequestArgs, response *ResponseArgs) error {
        return Put(request, response)
}

func (t *Request) PutConfirm(request RequestArgs, response *ResponseArgs) {
}

func (t *Request) Get(request RequestArgs, response *ResponseArgs) {
}

func (t *Request) Delete(request RequestArgs, response *ResponseArgs) {
}

func (t *Request) List(request RequestArgs, response *ResponseArgs) {
}

func Put(request RequestArgs, response *ResponseArgs) error {
	return nil        
}
