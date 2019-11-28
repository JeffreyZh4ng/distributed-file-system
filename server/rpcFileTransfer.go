package server

import (
	"ioutil"
	"time"
)

var FILE_RPC_PORT string = "7000"
var FOLDER_NAME string = "nodeFiles/"

// Holds arguments to be passed to service request in RPC call
type FileTransferRequest struct {
	FileName string,
	NodeGroup []string,
	Data []byte,
}

// When a file is resharded to another node, we need to update all the other nodes in the
// NodeGroup and update their nodeGroup lists
type UpdateNodeGroupRequest struct {
	FileName string,
	NodeGroup []string,
}

// Represents service Request
type FileTransfer int

func (t *FileTransfer) SendFile(request FileTransferRequest, _) error {
	filePath := FOLDER_NAME + request.FileName
	fileDes, _ := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	fileDes.Write(request.Data)

	LocalFiles.Files[request.FileName] = request.NodeGroup
	LocalFiles.UpdateTimes[request.FileName] = time.Now().UnixNano() / int64(time.Millisecond)

	return nil
}

func (t *FileTransfer) GetFile(request FileTransferRequest, data *[]byte) error {
	filePath := FOLDER_NAME + request.FileName
	fileContents, err := ioutil.ReadFile(filePath)
	*data = fileContents
 
	return nil
}

func (t *FileTransfer) UpdateNodeGroup(request UpdateNodeGroupRequest, _) error {
	LocalFiles.Files[request.FileName] = request.NodeGroup
	LocalFiles.UpdateTimes[request.FileName] = time.Now().UnixNano() / int64(time.Millisecond)

	return nil
}