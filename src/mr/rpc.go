package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type RegisterWorkerArg struct {
}

type RegisterWorkerReply struct {
	Id int
}

// Add your RPC definitions here.
type PullTaskArg struct {
}

type PullTaskReply struct {
	Task *Task
}

type ReportTaskArg struct {
	Task              *Task
	intermediateFiles []string
}

type ReportTaskReply struct {
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
