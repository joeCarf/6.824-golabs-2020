package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
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

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

func generateStartTime() int64 {
	now := time.Now()
	return now.Unix()
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
		ok := call("Master.AskForTask", &request, &response)
		if !ok {
			time.Sleep(1 * time.Second)
			continue
		}
		switch response.Task.Type {
		case MapTask:
			doMappingTask(mapf, &(response.Task))
			sendTaskDoneMessage(int(response.Task.TaskId), MapTask)
		case ReduceTask:
			doReducingTask(reducef, &(response.Task))
			sendTaskDoneMessage(int(response.Task.TaskId), ReduceTask)
		case Sleep:
			time.Sleep(500 * time.Millisecond)
		case None:
			return
		}
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func doMappingTask(mapf func(string, string) []KeyValue, task *Task) {
	//fmt.Println("[MAP-Do-]", task.TaskId, " filename=", task.FileName, "======", os.Getpid(), "  time: ", generateStartTime())
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
	//fmt.Println("[MAP] map task done...filename=", task.FileName, "  taskid=", task.TaskId, "======", os.Getpid(), "  time: ", generateStartTime())

}

func doReducingTask(reducef func(string, []string) string, task *Task) {
	//fmt.Println("[REDUCING] doing reduce task...", task)

	filenames := generateReducingFileList(task.FileName, task.TaskId)
	intermediate := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	sort.Sort(ByKey(intermediate))
	var outputFileName string = "mr-out-" + strconv.Itoa(int(task.TaskId))
	var outputContent string
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

	if err := writeAtomic(outputFileName, []byte(outputContent)); err != nil {
		log.Fatalln("worker.WorkReduce: writeAtomic failed!")
	}

	//fmt.Println("[REDUCING] reduce task done...", task)
}

func generateReducingFileList(maxMapTaskIdStr string, id int32) []string {
	var filenameList []string
	maxMapTaskId, _ := strconv.Atoi(maxMapTaskIdStr)
	for i := 0; i < maxMapTaskId; i++ {
		filename := intermediateFilePrefix + strconv.Itoa(i) + "-" + strconv.Itoa(int(id))
		filenameList = append(filenameList, filename)
	}
	return filenameList
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
