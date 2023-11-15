package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const TimeOut = 10 * time.Second

//===================定义Task====================//
type Task struct {
	Type      TaskType //任务类型
	Id        int      //任务ID
	Metadata  []string //任务中要处理的元数据	TODO: 这里简单起见定义为了[]string, 是否可以通过某种编码, 让他既可以处理string又可以处理[]string;
	NReduce   int      //nreduce
	Done      bool     //标识任务是否完成
	StartTime int64    //开始时间的标志
}

//任务类型
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	ExitTask
	SleepTask
)

//定义RPC通信, worker从Master这里拿到task

type Master struct {
	// Your definitions here.
	TaskId   int          //当前任务的编号
	Status   MasterStatus //当前所处的状态
	TaskChan chan *Task   //用来拿任务的chan
	NReduce  int          //reduce的个数
	TaskList TaskQueue    //存储了所有task实例
	mu       sync.RWMutex //读写锁Master实例, 防止竞争
	cond     *sync.Cond   //条件变量, 保证访问Master互斥
}

// 线程安全的任务队列
type TaskQueue struct {
	Queue map[int]*Task
	mu    sync.RWMutex
}

func (t *TaskQueue) enqueue(id int, task Task) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.Queue[id] = &task
}
func (t *TaskQueue) deque(id int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.Queue, id)
}

type MasterStatus int

const (
	MapStatus MasterStatus = iota
	ReduceStatus
	DoneStatus
	ExitStatus
)

type TaskArray struct {
}

//========================供worker调用的RPC处理方法=======================//
// Your code here -- RPC handlers for the worker to call.
//
// RequestTask
//  @Description: RequestTask从chan中取出一个任务执行, 这个是并发的,RPC是新开了协程执行的, 所以要考虑并发读写的问题
//  @receiver m
//  @param args
//  @param reply
//  @return error
//
func (m *Master) RequestTask(args *TaskArgs, reply *TaskReply) error {
	//NOTE: 这里为什么拿了锁, 会死锁, 用读写锁解决这个问题吗? 问题不是锁, 是chan缓冲不足, 频繁的Request导致SleepTask塞满了chan, 导致的死锁
	m.mu.RLock()
	defer m.mu.RUnlock()
	//根据当前的状态, 分配任务;
	//TODO: 发现所有操作都一样，为什么不直接合并呢?
	//DPrintf(dLog, "master.RequestTask: current goroutine ID: %d", GetGoroutineID())
	switch m.Status {
	case MapStatus:
		if len(m.TaskChan) > 0 {
			//从里面拿一个任务给他
			temp := <-m.TaskChan
			//NOTE: 这里不能创建新对象, 必须直接填充原对象
			reply.Type = temp.Type
			reply.Id = temp.Id
			reply.Metadata = temp.Metadata
			reply.NReduce = temp.NReduce
			//reply = &TaskReply{
			//	Type:     temp.Type,
			//	Id:       temp.Id,
			//	Metadata: temp.Metadata,
			//	NReduce:  temp.NReduce,
			//}
			DPrintf(dLog, "master.RequestTask: task is %v", reply)
			return nil
		} else {
			//如果拿不出来, 就可以通知work休息一会儿
			reply.Type = SleepTask
			reply.Id = 0
		}
	case ReduceStatus:
		//操作流程通Map阶段,也是拿一个任务给他, 拿不出来就让它休息
		if len(m.TaskChan) > 0 {
			//从里面拿一个任务给他
			temp := <-m.TaskChan
			reply.Type = temp.Type
			reply.Id = temp.Id
			reply.Metadata = temp.Metadata
			reply.NReduce = temp.NReduce
			DPrintf(dLog, "master.RequestTask: task is %v", reply)
			return nil
		} else {
			//如果拿不出来, 就可以通知work休息一会儿
			reply.Type = SleepTask
			reply.Id = 0
		}
	case DoneStatus:
		//操作流程通Map阶段,也是拿一个任务给他, 拿不出来就让它休息
		if len(m.TaskChan) > 0 {
			//从里面拿一个任务给他
			temp := <-m.TaskChan
			reply.Type = temp.Type
			reply.Id = temp.Id
			reply.Metadata = temp.Metadata
			reply.NReduce = temp.NReduce
			DPrintf(dLog, "master.RequestTask: task is %v", reply)
			return nil
		} else {
			//如果拿不出来, 就可以通知work休息一会儿
			reply.Type = SleepTask
			reply.Id = 0
		}
	default:
		//默认就发Sleep
		reply.Type = SleepTask
		reply.Id = 0
	}
	DPrintf(dLog, "Master.RequestTask: task id is %d ", reply.Id)
	return nil
}

//
// NotifyDone
//  @Description: NotifyDone的RPC Handler, 将对应的task设置为done
//  @receiver m
//  @param args
//  @param reply
//  @return error
//
func (m *Master) NotifyDone(args *NotifyArgs, reply *NotifyReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	task := args.Task
	err := args.Err
	//检查task的状态是否完成, 完成了直接清出队列
	if task.Done && err == nil {
		DPrintf(dLog, "Master.NotifyDone: TaskList[%d] is Done!", task.Id)
		m.TaskList.deque(task.Id)
		//NOTE: 两种实现,事件驱动和时间驱动, 这里是每次完成一个Map，都去检查一下是否都Done
		m.cond.Broadcast()

	} else {
		//否则就是出了问题, Map没有成功, 需要重启找一个Woker去做, 这里由handlerTaskTimeout一起处理了, 所以这里什么都不做就行
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
	DPrintf(dLog, "Master.server: listen on %s", sockname)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	//主进程周期性调用, 检查是否所有任务完成, 就是看状态是否是DoneStatus
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Status == ExitStatus {
		DPrintf(dLog, "master.Done: All Map Reduce Task Finished, Master Exited!")
		ret = true
	}
	return ret
}

//
// checkAllDone
//  @Description: 周期性检查是否完成的线程
//  @receiver m
//
func (m *Master) checkAllDone() {
	m.mu.Lock()
	defer m.mu.Unlock()
	//周期性检查是否所有任务都结束了
	var err error
	for err == nil {
		DPrintf(dLog, "master.checkAllDone: start check done")
		//检查一下tasklist, 是否完成了所有任务, 其实就是tasklist是否空了
		if len(m.TaskList.Queue) == 0 {
			//表示都完成了
			m.convertToNextStatus()
			DPrintf(dLog, "checkAllDone: all task is done!")
			DPrintf(dLog, "checkAllDone: convert to next phase %v !", m.Status)
			if m.Status == ExitStatus {
				//只有是ExitStatus才可以退出
				break
			}
		}
		m.cond.Wait()
	}
}

//
// convertToNextStatus
//  @Description: 从当前阶段转到下一个阶段, 并做下一个阶段开始的初始化工作
//  @receiver m
//
func (m *Master) convertToNextStatus() {
	//这里不需要加锁, 是因为外层已经加了锁
	switch m.Status {
	case MapStatus:
		//下个阶段是ReduceStatus, 要放入NReduce个对应task
		m.Status = ReduceStatus
		for i := 0; i < m.NReduce; i++ {
			task := &Task{
				Type:      ReduceTask,
				Id:        m.TaskId,
				Metadata:  fillReduceTaskMetadata(i),
				NReduce:   m.NReduce,
				Done:      false,
				StartTime: time.Now().Unix(),
			}
			m.TaskList.enqueue(task.Id, *task)
			m.TaskChan <- task
			m.TaskId++
		}
	case ReduceStatus:
		//下一个阶段是DoneStatus, 所有的工作都结束了, 用ExitTask告诉Worker要退出了
		m.Status = DoneStatus
		//这里应该根据worker的个数, 放对应多个的task;
		nworer := 1
		for i := 0; i < nworer; i++ {
			task := &Task{
				Type:      ExitTask,
				Id:        m.TaskId,
				Metadata:  nil,
				NReduce:   m.NReduce,
				Done:      false,
				StartTime: time.Now().Unix(),
			}
			m.TaskList.enqueue(task.Id, *task)
			m.TaskChan <- task
			m.TaskId++
		}
	case DoneStatus:
		//下一个阶段是ExitStatus, 所有的Worker已经退出, 这里可以直接拜拜了
		m.Status = ExitStatus
	default:
	}
}

//
// fillReduceTaskMetadata
//  @Description: 填充ReduceTask的MetaData字段
//  @param i	代表当前是第几个ReduceTask
//  @return []string
//
func fillReduceTaskMetadata(i int) []string {
	//根据文件名的最后一个字段分配reduce task
	var ret []string
	path, _ := os.Getwd()
	dirPath := filepath.Join(path, "map-tmp")
	//获取目录下所有文件
	files, _ := ioutil.ReadDir(dirPath)
	//构造后缀字符串
	suffix := fmt.Sprintf("_%d", i)
	for _, file := range files {
		if strings.HasSuffix(file.Name(), suffix) {
			//判断这个文件, 有i的后缀
			filename := filepath.Join(dirPath, file.Name())
			ret = append(ret, filename)
		}
	}
	return ret
}

//
// handlerTaskTimeout
//  @Description: 专门用于, 处理一个task是否超时, 超时就重新发送这个任务
//  @receiver m
//  @param task
//
func (m *Master) handlerTaskTimeout() {
	//周期性唤醒自己检查是否任务超时了
	for {
		//如果都做完了就退出
		if m.Status == DoneStatus {
			return
		}
		if len(m.TaskList.Queue) > 0 {
			for _, task := range m.TaskList.Queue {
				curTime := time.Now().Unix()
				if curTime-task.StartTime >= 10 {
					DPrintf(dLog, "master.handlerTaskTimeout: task %d time out, resent!", task.Id)
					m.TaskChan <- task
					task.StartTime = curTime
				}
			}
		}
		time.Sleep(1 * time.Second)
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	nTask := len(files) + nReduce + 3 + 10 //任务总数, 包括files个Map, nReduce个Reduce, 3个Exit, 最后空出来10个预留给Sleep
	m := Master{
		TaskId:   1,
		Status:   MapStatus,
		TaskChan: make(chan *Task, nTask),
		NReduce:  nReduce,
		TaskList: TaskQueue{
			Queue: make(map[int]*Task),
			mu:    sync.RWMutex{},
		},
		mu: sync.RWMutex{},
	}
	m.cond = sync.NewCond(&m.mu)
	// Your code here.
	// 按照输入的文件, 创建对应的Map任务, Reduce任务放到下一个阶段去处理
	for _, file := range files {
		//每个文件, 创建一个Map任务
		//TODO: 一个更好的设计思想是, 初始化的时候，只创建一个基本信息, 在真正使用的地方, 比如RequestTask里, 才真正的创建Task的具体信息放到List里;
		id := m.TaskId
		task := &Task{
			Type:     MapTask,
			Id:       id,
			Metadata: []string{file},
			NReduce:  nReduce,
			Done:     false,
		}
		// 将任务放到缓存和Map任务管道
		m.TaskList.enqueue(task.Id, *task)
		m.TaskChan <- task
		m.TaskId++
		//创建一个监听交易是否超时的协程
	}
	m.server()
	go m.checkAllDone()       //检查任务是否完成
	go m.handlerTaskTimeout() //检查是否超时
	return &m
}
