package server

import (
	"os"
	"time"
	"net"
	log "github.com/sirupsen/logrus"
	"encoding/json"
	"strconv"
	"io"
)
var FILE_TRANSFER_PORT string = "43991"
var FILE_MESSAGE_PORT string = "23333"
var SDFS_DIR string = "/home/zx27/SDFS/"

// Every heartbeat will also contain a list of all files in the system
type Requests struct {
	LastUpdate int64
	List	   map[string][]string
}

type FileMsg struct{
	MsgType string
	FileName string
	Data []string
	SrcHost string
}

func TCPFileServer(filePath string, destList *net.TCPListener, numReplicas int){
	for i := 0; i < numReplicas; i++{
		rdfile, err:= os.Open(filePath)
		if err != nil {
			log.Infof("Unable to open local file: %s", err)
			return
		}
		destConn, err := destList.Accept()
		if err != nil {
			log.Infof("Unable resolve file destination: %s", err)
			return
		}
		io.Copy(destConn, rdfile)
		destConn.Close()
		rdfile.Close()
	}
}

func TCPFileReceiver(filePath string, readConn *net.TCPConn){
	mkfile, err := os.Create(filePath)
	if err != nil {
		log.Infof("Unable to create local file: %s", err)
		return
	}
	io.Copy(mkfile, readConn)
	if err != nil {
		log.Infof("Unable to create local file: %s", err)
		return
	}
	mkfile.Close()
}

func ListenForFileMsg(l *net.TCPListener, membership *Membership, fileGroup map[string][]string, fileInfo map[string][]int64 ){
	for{
		fmconn, err := l.AcceptTCP()
		if err != nil {
			log.Fatal("TCP listener accept error ", err)
		}
		go handleFileMsg(fmconn, membership, fileGroup, fileInfo)
	}
}

func handleFileMsg(fmconn *net.TCPConn, membership *Membership, fileGroup map[string][]string, fileInfo map[string][]int64){
	defer fmconn.Close()
	connFrom := fmconn.RemoteAddr().String()
	log.Infof("Accept a new connection from: %s", connFrom)
	msgbuf := make([]byte, 1024)
		
	msglen, err := fmconn.Read(msgbuf)
	if err != nil {
		log.Infof("TCP read error %s", err)
		return
	}
	newFileMsg := FileMsg{}
	err = json.Unmarshal(msgbuf[:msglen], &newFileMsg)
	if err != nil {
		log.Infof("Could not decode file message! %s", err)
		return 
	}

	hostname, _ := os.Hostname()
	if hostname == membership.List[0]{
		masterManual(msgbuf, msglen, membership, fmconn, fileGroup, fileInfo)			
	}else{
		nodeManual(msgbuf, msglen, membership, fmconn, fileGroup, fileInfo)
	}
}
func nodeManual(msgbuf []byte, msglen int, membership *Membership, fmconn *net.TCPConn, fileGroup map[string][]string, fileInfo map[string][]int64){
	//localHost, _ := os.Hostname()
	newFileMsg := FileMsg{}
	err := json.Unmarshal(msgbuf[:msglen], &newFileMsg)
	if err != nil {
		log.Infof("Could not decode file message! %s", err)
		return 
	}

	switch newFileMsg.MsgType{
	case "put_from_master":
		curTime := time.Now().UnixNano() / int64(time.Millisecond)
		if _, exist := fileGroup[newFileMsg.FileName]; exist{
			if _, exist2 := fileInfo[newFileMsg.FileName]; exist2{
				fileInfo[newFileMsg.FileName][0] += 1 //update version
				fileInfo[newFileMsg.FileName][1] = curTime // last update time
			}else{ // 
				fileInfo[newFileMsg.FileName] = append(fileInfo[newFileMsg.FileName], 0)
				fileInfo[newFileMsg.FileName] = append(fileInfo[newFileMsg.FileName], curTime)
			}			
		}else{
			fileInfo[newFileMsg.FileName] = append(fileInfo[newFileMsg.FileName], 0)
			fileInfo[newFileMsg.FileName] = append(fileInfo[newFileMsg.FileName], curTime)
			fileGroup[newFileMsg.FileName] = newFileMsg.Data[1:]
		}
		ver := fileInfo[newFileMsg.FileName][0] //file version
		log.Infof("Getting file from %s: ", newFileMsg.Data[0])	
		tcpAddr, err := net.ResolveTCPAddr("tcp", newFileMsg.Data[0]+":"+FILE_TRANSFER_PORT)
		readConn, err := net.DialTCP("tcp",nil,tcpAddr)
		if err != nil {
			log.Infof("Cannot connect to file source: %s", err)
			return
		}
		localPath := SDFS_DIR+newFileMsg.FileName+"_v"+strconv.Itoa(int(ver))
		TCPFileReceiver(localPath, readConn)
		readConn.Close()
	
	default:
		log.Info("Invalid message type!")
		return	
	}

}
func masterManual(msgbuf []byte, msglen int, membership *Membership, fmconn *net.TCPConn, fileGroup map[string][]string, fileInfo map[string][]int64){
	localHost, _ := os.Hostname()
	newFileMsg := FileMsg{}
	err := json.Unmarshal(msgbuf[:msglen], &newFileMsg)
	if err != nil {
		log.Infof("Could not decode file message! %s", err)
		return 
	}

	switch newFileMsg.MsgType{
	case "put_init": // ffor unit test
		rpMsg := FileMsg{
			MsgType: "insert_confirm",
			FileName: "",
			Data: membership.List,
			SrcHost: localHost,
		}
		rpStr, _ := json.Marshal(rpMsg)
		fmconn.Write(rpStr)
		return

	case "put": 
		var targets []string
		putConfilict := false
		curTime := time.Now().UnixNano() / int64(time.Millisecond)
		if group, exist := fileGroup[newFileMsg.FileName]; exist{
			if _, exist2 := fileInfo[newFileMsg.FileName]; exist2{
				if curTime - fileInfo[newFileMsg.FileName][1] < 30000{
					putConfilict = true
				}else{
					fileInfo[newFileMsg.FileName][0] += 1 //update version
					fileInfo[newFileMsg.FileName][1] = curTime // last update time
					for _, replicaNode := range(group){
						targets = append(targets, replicaNode)
					}
				}				
			}else{ // 
				fileInfo[newFileMsg.FileName] = []int64{0}
				fileInfo[newFileMsg.FileName] = append(fileInfo[newFileMsg.FileName], curTime)
			}						
		}else{
			targets = newFileMsg.Data // That's a new file
			fileGroup[newFileMsg.FileName] = targets
			fileInfo[newFileMsg.FileName] = []int64{0}
			fileInfo[newFileMsg.FileName] = append(fileInfo[newFileMsg.FileName], curTime)
		}			
		
		// ask 3 nodes to fetch file from the original node

		rpMsg := FileMsg{}
		if putConfilict{
			rpMsg = FileMsg{
				MsgType: "put_prompt_confilict",
				FileName: "",
				Data: []string{},
				SrcHost: localHost,
			}			
		}else{
			rpMsg = FileMsg{
				MsgType: "put_prompt_ok",
				FileName: "",
				Data: targets,
				SrcHost: localHost,
			}
		}
		rpStr, err := json.Marshal(rpMsg)
		if err != nil {
			log.Infof("Cannot encode file message respond: %s", err)
			return
		}
		fmconn.Write(rpStr)
		resbuf := make([]byte, 1024)
		fmconn.SetReadDeadline(time.Now().Add(time.Second * 10))
		reslen, err := fmconn.Read(resbuf)
		if err != nil {
			log.Infof("Cannot get put confirmation from client: %s", err)
			return
		}
		newFileConfirm := FileMsg{}
		err2 := json.Unmarshal(resbuf[:reslen], &newFileConfirm)
		if err2 != nil {
			log.Infof("Cannot decode put confirmation: %s", err)
			return
		}
		// need to set timeout to handle ignore
		if newFileConfirm.MsgType != "put_confirmation"{
			log.Infof("Client cancel the put request!")
			return
		}
		for _, target := range targets{
			if target != localHost{
				log.Infof("Put file %s to Node %s: ", newFileMsg.FileName, target)
				tcpAddr, err := net.ResolveTCPAddr("tcp", target+":"+FILE_MESSAGE_PORT)
				if err != nil {
					log.Infof("Cannot resolve TCP address: %s", err)
					continue
				}
				msgConn, err := net.DialTCP("tcp", nil, tcpAddr)
				if err != nil {
					log.Infof("Cannot reach target node: %s", err)
					continue
				}
				insertGroup := []string{newFileMsg.SrcHost}
				for _, t := range targets{
					insertGroup = append(insertGroup, t)
				}
				rqmsg := FileMsg{
					MsgType: "put_from_master",
					FileName: newFileMsg.FileName,
					Data: insertGroup,
					SrcHost: localHost,
				}
				rqStr, _ := json.Marshal(rqmsg)
				msgConn.Write(rqStr)

			}else{ //leader itself have needed replicas
				log.Infof("Getting file from %s: ", newFileMsg.SrcHost)
				tcpAddr, err := net.ResolveTCPAddr("tcp", newFileMsg.SrcHost+":"+FILE_TRANSFER_PORT)
				readConn, err := net.DialTCP("tcp", nil, tcpAddr)
				if err != nil {
					log.Infof("Cannot connect to file source: %s", err)
					continue
				}
				finfo, _ := fileInfo[newFileMsg.FileName]
				ver := finfo[0]
				localPath := SDFS_DIR+newFileMsg.FileName+"_v"+strconv.Itoa(int(ver))
				TCPFileReceiver(localPath, readConn)
				readConn.Close()
			}
		}
	case "get":
		fileTobeSent := newFileMsg.FileName
		fileDest := newFileMsg.SrcHost
		
		if err != nil {
			log.Infof("Cannot resolve TCP address: %s", err)
			return
		}
		if finfo, have := fileInfo[fileTobeSent]; have{
			ver := finfo[0]
			tcpAddr, err := net.ResolveTCPAddr("tcp", fileDest+":"+FILE_TRANSFER_PORT)
			destConn, err := net.DialTCP("tcp", nil, tcpAddr)
			localFilePath := SDFS_DIR + fileTobeSent + "_v"+strconv.Itoa(int(ver))
			rdfile, err:= os.Open(localFilePath)
			if err != nil {
				log.Infof("Unable to open local file: %s", err)
				return
			}
			io.Copy(destConn, rdfile)
		}else{
			rqmsg := FileMsg{
				MsgType: "read_error",
				FileName: fileTobeSent,
				Data: []string{},
				SrcHost: localHost,
			}
			rqStr, _ := json.Marshal(rqmsg)
			fmconn.Write(rqStr)
			log.Infof("File %s is not available", fileTobeSent)
		}
	case "delete":
		deleteFile(newFileMsg.FileName, fileGroup, fileInfo)
	case "newRep":
		// New replica generated 
	default:
		log.Infof("Invalid message type!")
		return		
	}
}

func deleteFile(fileName string, fileGroup map[string][]string, fileInfo map[string][]int64){
	if _, e := fileGroup[fileName]; e{
		delete(fileGroup, fileName)
	}
	if _, ee := fileInfo[fileName]; ee{
		ver := int(fileInfo[fileName][0])
		delete(fileInfo, fileName)
		for i := 0; i <= ver; i++{
			err := os.Remove(SDFS_DIR + fileName + "_v"+strconv.Itoa(ver))
			if err != nil{
				continue
			}
		}
	}

}

func CheckReplicate(fileGroup map[string][]string, fileInfo map[string][]int64, membership *Membership){
	localHost, _ := os.Hostname()
	if len(membership.List) < 4{
		return
	}
	for file, group := range fileGroup{
		ver := int(fileInfo[file][0])
		localFilePath := SDFS_DIR + file + "_v"+strconv.Itoa(ver)
		if len(group) < 4 && group[len(group)-1] == localHost{
			Replicate(membership, fileGroup, file, localFilePath)
		}
	}
}

func Replicate(membership *Membership, fileGroup map[string][]string, file string, localFilePath string){
	localHost, _ := os.Hostname()
	index := findHostnameIndex(membership, localHost)
	for i:=0; i < len(membership.List); i++{
		next := membership.List[index]
		if !findGroupMember(next, fileGroup[file]){
			msg := FileMsg{
				MsgType: "replicate",
				FileName: file,
				Data: []string{},
				SrcHost: localHost,
			}
			newtcpAddr, err := net.ResolveTCPAddr("tcp", next+":"+FILE_MESSAGE_PORT)
			fconn, err := net.DialTCP("tcp", nil, newtcpAddr)
			jsonmsg, _ := json.Marshal(msg)
			fconn.Write(jsonmsg)
			tcpAddr, err := net.ResolveTCPAddr("tcp", ":"+FILE_TRANSFER_PORT)
			flistener, err := net.ListenTCP("tcp", tcpAddr)
			if err != nil{
				log.Infof("TCP listen error during replicate, %s", err)
				return
			}
			TCPFileServer(localFilePath, flistener, 1)
			buf := make([]byte,1024)
			fconn.Read(buf)
			fileGroup[file] = append(fileGroup[file], next)
			break
		}else{
			index = (index + 1)% len(membership.List)
		}
	}
}

func findGroupMember(host string, group []string) bool{
	for _, g := range group{
		if g == host{
			return true
		}
	}
	return false
}




