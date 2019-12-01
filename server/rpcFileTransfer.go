package server

import (
	log "github.com/sirupsen/logrus"
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

func (t *FileTransfer) SendFile(request FileTransferRequest, _ *TransferResult) error {
	filePath := SERVER_FOLDER_NAME + request.FileName
	fileDes, _ := os.OpenFile(filePath, os.O_TRUNC|os.O_CREATE|os.O_RDWR, 0666)
	fileDes.Write(request.Data)
	log.Infof("Wrote file %s to server!", request.FileName)

	LocalFiles.Files[request.FileName] = request.FileGroup
	LocalFiles.UpdateTimes[request.FileName] = time.Now().UnixNano() / int64(time.Millisecond)

	return nil
}

func (t *FileTransfer) GetFile(request FileTransferRequest, data *TransferResult) error {
	filePath := SERVER_FOLDER_NAME + request.FileName
	fileContents, _ := ioutil.ReadFile(filePath)
	*data = fileContents
	log.Infof("Sending file %s to client!", request.FileName)
 
	return nil
}

func (t *FileTransfer) UpdateFileGroup(request UpdateFileGroupRequest, _ *TransferResult) error {
	LocalFiles.Files[request.FileName] = request.FileGroup
	LocalFiles.UpdateTimes[request.FileName] = time.Now().UnixNano() / int64(time.Millisecond)

	return nil
}