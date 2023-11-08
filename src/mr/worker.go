package mr

import (
	"fmt"
	"io/ioutil"
	"os"
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var localCache [][]KeyValue

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	fmt.Println("worker.Worker: start request tast from master")
	var err error
	for err == nil {
		task := RequestTaskFromMaster()
		switch task.Type {
		case MapTask:
			WorkMap(mapf, task)
		default:

		}

	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// RequestTaskFromMaster
//  @Description: 通过RPC调用Master的RequestTask方法
//
func RequestTaskFromMaster() *Task {
	fmt.Println("worker.RequestTaskFromMaster: ")
	var args TaskArgs
	reply := TaskReply{}
	err := call("Master.RequestTask", &args, &reply)
	if err {
		log.Fatalln("RequestTaskFromMaster Failed", err)
	}
	return (*Task)(&reply)
}

func NotisfyMasterTaskDone() bool {
	fmt.Println("worker.NotisfyMasterTaskDone: ")
	var args TaskArgs
	reply := TaskReply{}
	ok := call("master.NotisfyDone", &args, &reply)
	if ok {
		log.Fatalln("NotisfyMasterTaskDone Failed")
	}
	return ok
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

//
// WorkMap
//  @Description: 执行文件切分操作Map, 从文件中统计词频, 并放到n个reduce的切片中
//  @param mapf
//  @param task
//  @return error
//
func WorkMap(mapf func(string, string) []KeyValue, task *Task) error {
	//根据文件元数据拿到文件
	filename := task.Metadata
	//和mrsequential.go中处理逻辑一样, 只是这里只处理一个文件
	var intermediate []KeyValue
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return err
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	//将这个一维的结果, 按照hash负载均衡到所有的reduce中
	nreduce := task.NReduce
	hashkv := make([][]KeyValue, nreduce)
	for _, kv := range intermediate {
		hashkv[ihash(kv.Key)%nreduce] = append(hashkv[ihash(kv.Key)%nreduce], kv)
	}
	localCache = hashkv
	fmt.Printf("worker.WorkMap: Task %d was Mapped!", task.Id)
	return nil
}
