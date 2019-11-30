package server

import (
	"io/ioutil"
	"os"
	"time"
)

var FILE_RPC_PORT string = "7000"
var SERVER_FOLDER_NAME string = "serverFiles/"

// Holds arguments to be passed to service request in RPC call
type FileTransferRequest struct {
	FileName string
	FileGroup []string
	Data []byte
}

// When a file is resharded to another node, we need to update all the other nodes in the
// FileGroup and update their FileGroup lists
type UpdateFileGroupRequest struct {
	FileName string
	FileGroup []string
}

// Represents service Request
type FileTransfer int

type TransferResult []byte

func (t *FileTransfer) SendFile(request FileTransferRequest, _ *Result) error {
	filePath := SERVER_FOLDER_NAME + request.FileName
	fileDes, _ := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	fileDes.Write(request.Data)

	LocalFiles.Files[request.FileName] = request.FileGroup
	LocalFiles.UpdateTimes[request.FileName] = time.Now().UnixNano() / int64(time.Millisecond)

	return nil
}

func (t *FileTransfer) GetFile(request FileTransferRequest, data *Result) error {
	filePath := SERVER_FOLDER_NAME + request.FileName
	fileContents, _ := ioutil.ReadFile(filePath)
	*data = fileContents
 
	return nil
}

func (t *FileTransfer) UpdateFileGroup(request UpdateFileGroupRequest, _ *Result) error {
	LocalFiles.Files[request.FileName] = request.FileGroup
	LocalFiles.UpdateTimes[request.FileName] = time.Now().UnixNano() / int64(time.Millisecond)

	return nil
}