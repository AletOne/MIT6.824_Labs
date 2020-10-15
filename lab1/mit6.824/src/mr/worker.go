package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import (
	"os"
	"io/ioutil"
	"strconv"
	"encoding/json"
	"sort"
	"time"
	"errors"
	"syscall"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		reply, err := RequestTask()
		if err != nil {
			continue
		}
		// fmt.Println(reply) 
		switch reply.Flag {
		case NoTaskGetted:
			time.Sleep(time.Second)
			continue
		case MasterCompleted:
			return
		case TaskGetted:
			if reply.Task.TaskType  == MapTask {
				DoMapTask(mapf, reply.Task)
				UpdateTask(reply.Task.TaskType, reply.Task.Id)
			} else {
				DoReduceTask(reducef, reply.Task)
				UpdateTask(reply.Task.TaskType, reply.Task.Id)
			}	
			time.Sleep(time.Second)
		default:
			panic("worker")
		}
	}
	
	// uncomment to send the Example RPC to the master.
	// CallExample()
}

// Map task 
func DoMapTask(mapf func(string, string) []KeyValue, task Task) {
	// 读取文件内容
	file, err := os.Open(task.Filename)
	defer file.Close()
	if err != nil {
		fmt.Println("Cannot open %v", task.Filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Println("Cannot read %v", task.Filename)
	}

	// 将读取到的内容传递给 Map 函数，生成一系列 key/value
	kva := mapf(task.Filename, string(content))

	// Map 输出的中间结果进行分类
	grounps := make(map[int][]KeyValue, task.NReduce)
	for _, kv := range kva {
		n := ihash(kv.Key) % task.NReduce
		grounps[n] = append(grounps[n], kv)
	}
	
	// Map 的中间结果输出到中间文件中
	nReduce := task.NReduce
	// Hint: 这边的循环不能用 groups，因为 groups 中不一定包含所有的 nReduce 个 key。
	// 在发生 crash 的时候，比如刚开始一个 task 中的 groups 中只有几个 key，生成的文件是 mr-1-1, mr-1-2；
	// 之后一个 task 中的 groups 中也只有几个 key，生成的文件是 mr-1-3, mr-1-4，那么不同 task 生成的文件
	// 被当成了同一个 task 生成的文件，会存在不一致性
	for i := 0; i < nReduce; i++ {	
		// interFilename := "mr-" + strconv.Itoa(task.Id) + "-" + strconv.Itoa(i)
		interFilename := intermediateFilename(task.Id, i)

		interFile, err := os.Create(interFilename)
		if err != nil {
			return
		}
		defer file.Close()

		if err := syscall.Flock(int(interFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			return
		}
		defer syscall.Flock(int(interFile.Fd()), syscall.LOCK_UN)

		enc := json.NewEncoder(interFile)
		for _, kv := range grounps[i] {
			enc.Encode(&kv)
		}
	}
}

// Reduce task
func DoReduceTask(reducef func(string, []string) string, task Task) {
	// 读取中间文件的内容
	// 因为 reduce 是在 map 之后开始的，因此中间文件全部都产生了
	kvMap := map[string][]string{}
	nMap := task.NMap
	for i := 0; i < nMap; i++ {
		// filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(task.Id)
		filename := intermediateFilename(i, task.Id)

		file, err := os.Open(filename)
		defer file.Close()

		if err != nil {
			fmt.Println("Open " + filename + " failed")
			return
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
		}
	}

	// 进行排序
	keys := []string{}
	for key,_ := range kvMap {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	
	// 读取的内容依次传递给 Reduce
	// 输出的内容写入文件中
	outputFile, _ := os.Create("mr-out-" + strconv.Itoa(task.Id))
	defer outputFile.Close()

	syscall.Flock(int(outputFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	defer syscall.Flock(int(outputFile.Fd()), syscall.LOCK_UN)
	for _, key := range keys {
		countString := reducef(key, kvMap[key])
		outputFile.WriteString(fmt.Sprintf("%v %v\n", key, countString))
	}
}

func intermediateFilename(i ,j int) string {
	return "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(j)
}

func RequestTask() (RequestTaskReply, error){
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	ret := call("Master.RequestTask", &args, &reply)
	if ret {
		return reply, nil
	}
	return RequestTaskReply{}, errors.New("Connect Failed!")
}

func UpdateTask(taskType, taskId int) {
	args := UpdateTaskArgs{
		TaskType: taskType,
		TaskId: taskId,
	}
	reply := UpdateTaskReply{}
	call("Master.UpdateTask", &args, &reply)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
