package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

//get task rpc 
type GetTaskRequest struct {
	Index int
}

type GetTaskReply struct {
	//State 0 sleep 1 map 2 reduce 3 finish
	TaskType int
	// file name to store Map result,used both in map and reduce
	TaskName string
	// reduce worker num
	ReduceNum int 

	// num of this worker
	Index int

	InputName []string
	//OutputName []string

}
//response task rpc
type FinishTaskRequest struct {
	TaskType int
	TaskName string
	FileName  []string

}

type FinishTaskReply struct {
	Repltype int
}








// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
