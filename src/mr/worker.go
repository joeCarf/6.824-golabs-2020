package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"
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
	DPrintf(dLog, "worker.Worker: start request tast from master")
	var err error
	for err == nil {
		task := RequestTaskFromMaster()
		switch task.Type {
		case MapTask:
			//处理Map任务, 处理完了通知Master
			err := WorkMap(mapf, task)
			if err != nil {
				log.Fatalln("worker.WorkMap task %d error %v", task.Id, err)
				return
			}
			NotifyMasterTaskDone(task, err)
		case ReduceTask:
			//处理Reduce任务, 处理完毕通知Master
			err := WorkReduce(reducef, task)
			if err != nil {
				log.Fatalln("worker.WorkReduce task %d error %v", task.Id, err)
				return
			}
			NotifyMasterTaskDone(task, err)
		case SleepTask:
			//如果是SleepTask, 说明要休眠10s
			time.Sleep(time.Second * 1)
		case ExitTask:
			//Master通知你要退出了
			task.Done = true
			var err error
			NotifyMasterTaskDone(task, err)
			DPrintf(dWorker, "worker.Worker: work exit!!!")
			//NOTE: 这里不能用break, 得用return
			//NOTE: Worker的退出机制有两种, 一种是RPC调用失败退出, 另一种是收到Master给他的ExitTask, 这种会有问题, master得准备N个ExitTask, 并且要等Worker们都拿到才能退出; 保证chan能访问到
			//但是这个Worker的数量就得提前知道，他的每个test里面的worker数不一样的;
			return
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
	DPrintf(dLog, "worker.RequestTaskFromMaster: send rpc")
	var args TaskArgs
	reply := TaskReply{}
	ok := call("Master.RequestTask", &args, &reply)
	if !ok {
		log.Fatalln("worker.RequestTaskFromMaster: rpc Failed")
	}
	DPrintf(dLog, "worker.RequestTaskFromMaster: reply is %v", reply)
	return &Task{
		Type:     reply.Type,
		Id:       reply.Id,
		Metadata: reply.Metadata,
		NReduce:  reply.NReduce,
		Done:     reply.Done,
	}
}

func NotifyMasterTaskDone(task *Task, err error) bool {
	DPrintf(dLog, "worker.NotifyMasterTaskDone: send rpc")
	args := NotifyArgs{
		Task: *task,
		Err:  err,
	}
	var reply NotifyReply
	ok := call("Master.NotifyDone", &args, &reply)
	if !ok {
		log.Fatalln("NotifyMasterTaskDone Failed")
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
	filename := task.Metadata[0]
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
	// 将文件保存到./map-tmp目录下
	path, err := os.Getwd()
	newDir := filepath.Join(path, "map-tmp")
	if _, err := os.Stat(newDir); os.IsNotExist(err) {
		// 如果不存在，则创建目录
		err := os.Mkdir(newDir, 0755)
		if err != nil {
			return err
		}
	}
	// crash要求, 写入文件是原子的, 用writeAtomic来实现
	for i, kv := range hashkv {
		filename := filepath.Join(newDir, fmt.Sprintf("map_tmp_%d_%d", task.Id, i))
		jsonData, err := json.Marshal(kv)
		err = writeAtomic(filename, jsonData)
		//err = ioutil.WriteFile(filename, jsonData, 0644)
		if err != nil {
			log.Fatalln("worker.WorkMap: write to fail failed", filename)
		}
	}
	//DPrintf(dMap, "worker.workMap: hashkv is written to file")
	//localCache = hashkv
	task.Done = true
	DPrintf(dMap, "worker.WorkMap: Task %d was Mapped!", task.Id)
	return nil
}

//
// WorkReduce
//  @Description: 实际执行Reduce的操作, 合并一位维切片里的值计算结果写入mr-tmp文件中
//  @param reducef
//  @param task
//  @return error
//
func WorkReduce(reducef func(string, []string) string, task *Task) error {
	intermediate := Shuffle(task.Metadata)
	// ==================mrsequential.go中的Reduce操作======================//
	oname := fmt.Sprintf("mr-out-%d", task.Id%task.NReduce)
	//ofile, _ := os.Create(oname)
	var outputContent string
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		//fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		outputContent += fmt.Sprintf("%v %v\n", intermediate[i].Key, output)

		i = j
	}
	if err := writeAtomic(oname, []byte(outputContent)); err != nil {
		log.Fatalln("worker.WorkReduce: writeAtomic failed!")
		return err
	}
	//============================================================================//
	DPrintf(dReduce, "worker.WorkReduce: Task %d was Reduced to %v!", task.Id, oname)
	task.Done = true
	return nil
}

type ByKey []KeyValue

//TODO: 没懂为什么不定义下面的方法就不行, 原来是如何运行的
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Shuffle
//  @Description: 根据给的所有文件，从中JSON读出[]KeyValue, 放到一个里面返回
//  @param filePaths
//  @return []KeyValue
//
func Shuffle(filePaths []string) []KeyValue {
	var ret []KeyValue
	for _, filePath := range filePaths {
		fileData, err := ioutil.ReadFile(filePath)
		if err != nil {
			log.Fatalf("worker.Shuffle: read file %v error %v", filePath, err)
		}
		var fileContent []KeyValue
		_ = json.Unmarshal(fileData, &fileContent)
		ret = append(ret, fileContent...)
	}
	//sort排好序
	sort.Sort(ByKey(ret))
	return ret
}

//
// getFuncName
//  @Description: 根据调用栈, 拿到当前函数名
//  @return string
//
func getFuncName() string {
	pc, _, _, ok := runtime.Caller(1)
	if !ok {
		fmt.Println("No caller information")
		return ""
	}

	fn := runtime.FuncForPC(pc)
	if fn == nil {
		fmt.Println("No func information")
		return ""
	}
	return fn.Name()
}

//
// writeAtomic
//  @Description: 通过tempFile, 实现文件写入的原子操作
//  @param filename
//  @param data
//  @return error
//
func writeAtomic(filename string, data []byte) error {
	//创建临时文件
	tempFile, err := ioutil.TempFile(filepath.Dir(filename), "temp")
	//退出前保证关闭临时文件
	defer tempFile.Close()
	if err != nil {
		log.Fatalln("open temp file error", err)
		return err
	}
	//将数据写入临时文件
	if _, err := tempFile.Write(data); err != nil {
		return err
	}
	//将临时文件改为需要写入的文件
	err = os.Rename(tempFile.Name(), filename)
	return err
}
