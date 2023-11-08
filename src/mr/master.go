package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

//===================定义Task====================//
type Task struct {
	Type     TaskType //任务类型
	Id       int      //任务ID
	Metadata string   //任务中要处理的元数据
	NReduce  int      //nreduce
}

//任务类型
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
)

//定义RPC通信, worker从Master这里拿到task

type Master struct {
	// Your definitions here.
	TaskId     int          //当前任务的编号
	Status     MasterStatus //当前所处的状态
	MapChan    chan *Task   //Map任务的通道
	ReduceChan chan *Task   //Reduce任务的通道
	NReduce    int          //reduce的个数
	mu         sync.Mutex   //锁Master实例, 防止竞争
}
type MasterStatus int

const (
	MapStatus MasterStatus = iota
	ReduceStatus
	DoneStatus
)

//========================供worker调用的RPC处理方法=======================//
// Your code here -- RPC handlers for the worker to call.
//
// RequestTask
//  @Description: RequestTask从chan中取出一个任务执行
//  @receiver m
//  @param args
//  @param reply
//  @return error
//
func (m *Master) RequestTask(args *TaskArgs, reply *TaskReply) error {
	//根据当前的状态, 分配任务
	switch m.Status {
	case MapStatus:
		if len(m.MapChan) > 0 {
			//从里面拿一个任务给他
			reply = (*TaskReply)(<-m.MapChan)
			return nil
		} else {
			//TODO: 拿不出来怎么处理
		}
	default:
		//TODO: Reduce和Done阶段都未实现
	}
	fmt.Println("Master.RequestTask: task id is : ", reply.Id)
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
	fmt.Println("Master.server: listen on ", sockname)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	//只需要检查是否所有的task都完成了
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		TaskId:     0,
		Status:     MapStatus,
		MapChan:    make(chan *Task, len(files)),
		ReduceChan: nil,
		NReduce:    nReduce,
	}
	// Your code here.
	// 按照输入的文件, 创建对应的Map任务;
	//TODO: 后面还应该有Reduce任务
	for _, file := range files {
		//每个文件, 创建一个Map任务
		id := m.TaskId
		task := &Task{
			Type:     MapTask,
			Id:       id,
			Metadata: file,
			NReduce:  nReduce,
		}
		// 放入Map任务管道
		m.MapChan <- task
		m.TaskId++
	}
	m.server()
	return &m
}
