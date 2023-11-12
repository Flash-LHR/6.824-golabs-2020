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

type MapTaskReply struct {
	NReduce       int
	RunID         int
	InputFileName string
}

type ReduceTaskReply struct {
	NMap           int
	RunID          int
	TaskID         int
	InputFileNames []string
}

type AssignTaskArgs struct {
}

type AssignTaskReply struct {
	TaskType   TaskTypeEnum
	MapTask    MapTaskReply
	ReduceTask ReduceTaskReply
}

type FinishMapTaskArgs struct {
	RunID                 int
	IntermediateFileNames []string
}

type FinishMapTaskReply struct {
}

type FinishReduceTaskArgs struct {
	RunID          int
	outputFileName string
}

type FinishReduceTaskReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
