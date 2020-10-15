package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// RequestTaskReply.Flag
const (
	TaskGetted = iota
	NoTaskGetted
	MasterCompleted
)

type RequestTaskArgs struct {}

type RequestTaskReply struct {
	Task Task	// request 到的任务
	Flag int	// response flag
}

type UpdateTaskArgs struct {
	TaskType int	// update task 的 TaskType 的值
	TaskId int		// update task 的 Id 的值
}

type UpdateTaskReply struct{}

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
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
