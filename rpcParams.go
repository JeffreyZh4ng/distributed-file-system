package rpcParams

//Holds arguments to be passed to service Arith in RPC call
type RequestArgs struct {
	SourceHost string
	Commmand string
	Data	[]byte
	DataLen int
}

type ResponseArgs struct {
	SourceHost string
	Command string
	Hosts	[]string
	Data	[]byte
	DataLen int
}

//Represents service Request
type Request int

//Result of RPC call is of this type
type Response ResponseArgs

func (t *Request) Put(request RequestArgs, response *ResponseArgs) {
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

//stores product of args.A and args.B in result pointer
func Put(request RequestArgs, response *ResponseArgs) {
	*result = Result(args.A * args.B)
	return nil
}
