package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.
type GetArgs struct{
	Message string //"ask for a task"
}

type GetReply struct{
	The_task Task // task type: map or reduce, taskFilename, taskID
	Map_finished_num int
	Err string // "error"
}

type PutArgs struct{
	Message string // "task finished"
	The_task Task // task type: map or reduce, taskFilename, taskID
	Err string // if task failed, return "error"
}

type PutReply struct{
	Message string
	Finish bool
	Err string
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
