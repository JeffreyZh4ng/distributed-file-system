# 1
Create two folders in the cs-425-mp3 root directory titled
- clientFiles
- serverFiles

 Put any files you want to upload inside of clientFiles

# 2
Start up all the servers of the sdfs using
- go run serverMain.go

The server also has a few commands
- id (Prints out the hostname of the server)
- list (Prints out the membership list)
- store (Prints out all the file names stored at the server)
- leave (The server will leave the network)

# 3
Use the client to make requests to the sdfs using
- go run clientMain.go (command) [args] ...

Example requests:
- go run clientMain.go put localFileName sdfsFileName
	-  "localFileName" is the local filename you want to upload to the sdfs and "sdfsFileName" is the name that you want for the file to have within the sdfs
- go run clientMain.go get sdfsFileName localFileName
	- The server will look for "sdfsFileName" inside of the sdfs and store it locally with the name "localFileName"
- go run clientMain.go delete sdfsFileName
	- The file "sdfsFileName" will be deleted from all servers in the sdfs
- go run clientMain.go ls sdfsFileName
	- The client will print out the hostnames of all servers that store the file "sdfsFileName"

NOTE - When making client requests, do not include clientFiles/ in the name of the local file