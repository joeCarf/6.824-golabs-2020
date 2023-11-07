package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	WorkingStatus     Status     // Master所处工作状态
	FileNames         []string   // 执行任务的文件名
	WorkingQueue      []Task     // 正在进行的任务队列
	CurrentTaskId     int32      // 当前正在进行的任务Id
	TaskIdMutex       sync.Mutex // 任务id自增时锁
	MapTaskChannel    chan *Task // 存放Map任务的channel
	ReduceTaskChannel chan *Task // 存放Reduce任务的channel

	NReduce int // 划分成多少个reduce任务
}

type Status = int32

const (
	Mapping = iota
	Reducing
	Done
)

type Task struct {
	StartTime int64    // 开始执行时间戳
	FileName  string   // 任务对应的文件名
	TaskId    int32    // 任务号
	Type      TaskType // 任务类型
	NReduce   int      // reduce任务个数（用来hash）
}

type TaskType = int32

const (
	MapTask = iota
	ReduceTask
	None
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

/**
 * 下面是暴露给worker的RPC接口
 */
func (m *Master) AskForTask(request *AskForTaskRequest, response *AskForTaskResponse) error {
	fmt.Println("receive ask for task from worker...")
	var task Task
	switch m.WorkingStatus {
	case Mapping:
		task, _ = m.processMappingTask()
	case Reducing:
		task, _ = m.processReducingTask()
	case Done:
		task = Task{Type: None}
	}

	response.Task = task
	fmt.Println("the task is: ", response.Task)
	return nil
}

func (m *Master) processMappingTask() (Task, error) {
	task := <-m.MapTaskChannel
	(*task).TaskId = m.generateTaskId()
	(*task).StartTime = m.generateStartTime()
	(*task).Type = MapTask
	(*task).NReduce = m.NReduce
	return *task, nil
}

func (m *Master) generateTaskId() int32 {
	var taskId int32

	m.TaskIdMutex.Lock()
	taskId = m.CurrentTaskId
	m.CurrentTaskId++
	m.TaskIdMutex.Unlock()

	return taskId
}

func (m *Master) generateStartTime() int64 {
	now := time.Now()
	return now.Unix()
}

func (m *Master) processReducingTask() (Task, error) {
	return Task{}, nil
}

func (m *Master) NotifyTaskDone(
	request *NotifyTaskDoneRequest,
	response *NotifyTaskDoneResponse) error {
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
	ret := false

	// Your code here.
	switch m.WorkingStatus {
	case Done:
		ret = true
	default:
		ret = false
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	var nMap = len(files) // Map任务个数
	m := Master{
		WorkingStatus:     Mapping,
		FileNames:         files,
		WorkingQueue:      make([]Task, nMap),
		CurrentTaskId:     0,
		MapTaskChannel:    make(chan *Task, nMap),
		ReduceTaskChannel: make(chan *Task, nReduce),
		NReduce:           nReduce,
	}

	for _, file := range files {
		var task = Task{FileName: file}
		m.MapTaskChannel <- &task
	}

	// Your code here.

	m.server()
	return &m
}
