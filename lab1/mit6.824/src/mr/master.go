package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import (
	"sync"
	"time"
)

// Task.TaskType
const (
	MapTask = iota	
	ReduceTask
)

// Task.State
const (
	TaskIdle = iota
	TaskInProgress
	TaskCompleted 
)

// Master.state
const (
	MRInit = iota		// MapReduce 初始化
	MRMapProgress 		// MapReduce 正在处理 map 任务
	MRReduceProgress	// lab 中要求 reduce task 要在 map 都完成之后开始
	MRCompleted			// 完成 MapReduce
)

// task data structure
type Task struct {
	TaskType int		// task 类型，map/reduce
	Id int				// task 编号
	State int			// task 的状态，idle/inProgress/completed
	Filename string		// task 处理的文件名，在这个 lab 主要是给 master
	NReduce int			// reduce 的数量，在对中间结果进行处理时需要
	NMap int			// map 的数量
	Time time.Time		// task 开始的时间
}

// master data structure
type Master struct {
	// Your definitions here.
	mapTasks []Task		// map tasks 记录
	reduceTasks []Task	// reduce tasks 记录
	nMap int 			// map 的数量
	nReduce	int 		// reduce 的数量
	state int			// master state
	mutex sync.Mutex    // 共享数据的一把锁
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master)RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error{
	/**
	 * master 在分配 task 的时候，一个是分配新的 task，另一个是将超时的 task 分配出去。
	 * 判断 task 是否超时：首先在分配出去的时候，设置一个时间戳，当又收到新的请求的时候，
	 * 判断出那些还在处理，但是超时的 task，将这个 task 分配给其他 worker。
	 */

	m.mutex.Lock()
	defer m.mutex.Unlock()
	switch m.state {
	case MRInit:
		reply.Flag = NoTaskGetted
	case MRMapProgress:
		for i, task := range m.mapTasks {
			// 分配暂时新的任务
			if task.State == TaskIdle {
				reply.Task.TaskType = task.TaskType
				reply.Task.Id = task.Id
				reply.Task.State = TaskInProgress
				reply.Task.Filename = task.Filename
				reply.Task.NReduce = task.NReduce
				reply.Flag = TaskGetted

				m.mapTasks[i].State = TaskInProgress
				m.mapTasks[i].Time = time.Now()
				
				return nil

			} else if task.State == TaskInProgress && time.Now().Sub(task.Time)  > 10 * time.Second{
				// 重新分配超时的任务
				reply.Task.TaskType = task.TaskType
				reply.Task.Id = task.Id
				reply.Task.State = task.State
				reply.Task.Filename = task.Filename
				reply.Task.NReduce = task.NReduce
				reply.Flag  = TaskGetted

				m.mapTasks[i].Time = time.Now()

				return nil
			}	
		}
		reply.Flag = NoTaskGetted
	case MRReduceProgress:
		for i, task := range m.reduceTasks {
			// 分配新的任务
			if task.State == TaskIdle {
				reply.Task.TaskType = task.TaskType
				reply.Task.Id = task.Id
				reply.Task.State = TaskInProgress
				reply.Task.Filename = task.Filename
				reply.Task.NMap = task.NMap
				reply.Flag = TaskGetted

				m.reduceTasks[i].Time = time.Now()
				m.reduceTasks[i].State = TaskInProgress
				
				return nil
			} else if task.State == TaskInProgress && time.Now().Sub(task.Time) > 10 * time.Second {
				// 重新分配超时的任务
				reply.Task.TaskType = task.TaskType
				reply.Task.Id = task.Id
				reply.Task.State = TaskInProgress
				reply.Task.Filename = task.Filename
				reply.Task.NMap = task.NMap
				reply.Flag = TaskGetted

				m.reduceTasks[i].Time = time.Now()	// 时间戳重新开始
				
				return nil
			}
		}
		reply.Flag = NoTaskGetted
	default:
		reply.Flag = MasterCompleted
	
	}

	return nil
}

// update task state
func (m *Master)UpdateTask(args *UpdateTaskArgs, reply *UpdateTaskReply) error {
	// add lock
	m.mutex.Lock()
	defer m.mutex.Unlock()

	// update task state
	if args.TaskType == MapTask {
		m.mapTasks[args.TaskId].State = TaskCompleted
	} else if args.TaskType == ReduceTask{
		m.reduceTasks[args.TaskId].State = TaskCompleted
	}

	// update master state
	switch m.state {
	case MRMapProgress:
		for _, task := range m.mapTasks {
			if task.State != TaskCompleted {
				return nil
			}
		}
		m.state = MRReduceProgress
	case MRReduceProgress:
		for _, task := range m.reduceTasks {
			if task.State != TaskCompleted {
				return nil
			}
		}
		m.state = MRCompleted
	default:
		m.state = MRCompleted
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	if m.state == MRCompleted {
		return true
	} else {
		return false
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		mapTasks: []Task{},
		reduceTasks: []Task{},
		nMap: len(files),
		nReduce: nReduce,
		state: MRInit,
		mutex: sync.Mutex{},
	}

	// 添加 Task，一个文件一个 Task
	for i, file := range files {
		m.mapTasks = append(m.mapTasks, Task {
			TaskType: MapTask,
			Id: i,
			State: TaskIdle,
			Filename: file,
			NReduce: m.nReduce, 
		})
	}

	// 添加 nReduce，一共 m.nReduce 个
	for i := 0; i < nReduce; i++ {
		m.reduceTasks = append(m.reduceTasks, Task{
			TaskType: ReduceTask,
			Id: i,
			State: TaskIdle,
			Filename: "",			// 已经有约定了
			NMap: m.nMap,
		})
	}

	m.state = MRMapProgress			// 更改 master.state 状态 

	m.server()
	return &m
}
