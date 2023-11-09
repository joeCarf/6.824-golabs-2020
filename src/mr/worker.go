package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type AskForTaskRequest struct {
}

type AskForTaskResponse struct {
	Task Task
}

type NotifyTaskDoneRequest struct {
	TaskId   int
	TaskType TaskType
}

type NotifyTaskDoneResponse struct {
}

const intermediateFilePrefix string = "mr-in-"

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
		request := AskForTaskRequest{}
		response := AskForTaskResponse{}
		call("Master.AskForTask", &request, &response)
		switch response.Task.Type {
		case MapTask:
			doMappingTask(mapf, &(response.Task))
			sendTaskDoneMessage(int(response.Task.TaskId), MapTask)
		case ReduceTask:
			doReducingTask(reducef, &(response.Task))
		case None:
			return
		}
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func doMappingTask(mapf func(string, string) []KeyValue, task *Task) {
	intermediate := []KeyValue{}
	filename := task.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	HashedKV := make([][]KeyValue, task.NReduce)

	for _, kv := range intermediate {
		index := ihash(kv.Key) % task.NReduce
		HashedKV[index] = append(HashedKV[index], kv)
	}

	for i := 0; i < task.NReduce; i++ {
		intermediateFileName := intermediateFilePrefix + strconv.Itoa(int(task.TaskId)) + "-" + strconv.Itoa(i)
		infile, _ := os.Create(intermediateFileName)
		enc := json.NewEncoder(infile)
		for _, kv := range HashedKV[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("err when encoding file %v", intermediateFileName)
			}
		}
	}

}

func doReducingTask(reducef func(string, []string) string, task *Task) {
	fmt.Println("received reduce task is: ", task)
}

func sendTaskDoneMessage(taskId int, taskType TaskType) {
	request := NotifyTaskDoneRequest{TaskId: taskId, TaskType: taskType}
	response := NotifyTaskDoneRequest{}
	call("Master.NotifyTaskDone", &request, &response)
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
