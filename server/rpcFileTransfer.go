package server

var FILE_RPC_PORT string = "7000"
var FOLDER_NAME string = "nodeFiles/"

// Holds arguments to be passed to service request in RPC call
type FileTransferRequest struct {
	FileName string,
	Data []byte,
}

// Hold arguments to be returned to the client
type FileTransferResponse struct {
	Data []byte,
}

// Represents service Request
type FileTransfer int

func (t *FileTransfer) SendFile(request FileTransferRequest, _) error {
	filePath := FOLDER_NAME + request.FileName
	fileDes, _ := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	fileDes.Write(request.Data)

	return nil
}

func (t *FileTransfer) GetFile(request FileTransferRequest, response *FileTransferResponse) error {
	filePath := FOLDER_NAME + request.FileName
	fileContents, err := ioutil.ReadFile(filePath)
	*response.Data = fileContents
 
	return nil
}