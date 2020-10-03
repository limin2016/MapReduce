package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	Unallocate      = 0
	Allocated       = 1
	Running         = 2
	Unfinished      = 3
	Finished        = 4
	MapTask         = 0
	ReduceTask      = 1
	LongestDuration = 10
)

type Task struct {
	TaskType   int
	MapFile    string
	Status     int
	Seq        int
	ReduceFile []string
	StartTime  time.Time
}

type Master struct {
	// Your definitions here.
	MapTaskNum          int
	MapTaskStatus       []int
	MapTaskStartTime    []time.Time
	ReduceTaskNum       int
	ReduceTaskStatus    []int
	ReduceTaskStartTime []time.Time
	MapTaskFinished     int
	ReduceTaskFinished  int
	Files               []string
	Tasks               chan Task
	// At the same time, only one thread can modify the master
	Mutex sync.Mutex
}

// // Don't use
// func (m *Master) handle(req MasterReq, resp *MasterResp) error {
// 	return nil
// }

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	DPrintf("begin server\n")
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	//socketname := masterSock()
	os.Remove("mr-socket")
	socketname := "mr-socket"
	l, e := net.Listen("unix", socketname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Serve(l, nil)
	DPrintf("end server\n")
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	ret := false
	// Your code here.
	if m.ReduceTaskFinished == Finished {
		ret = true
	}
	return ret
}

//RPC metohd. Worker request one maptask. This function will give one maptask
// to the Worker
func (m *Master) GetOneMapTask(reply *RequestTaskReply) {
	DPrintf("start GetOneMapTask\n")
	reply.MapTaskNum = m.MapTaskNum
	reply.ReduceTaskNum = m.ReduceTaskNum
	reply.GotTask = false
	if m.MapTaskFinished == Finished {
		// tell each worker map task has finished
		reply.MapTaskFinished = Finished
		return
	}
	// if there are avaliable tasks, then distribute one task to the worker
	if len(m.Tasks) > 0 {
		reply.ReplyTask = <-m.Tasks
		reply.GotTask = true
		reply.ReplyTask.Status = Running
		reply.ReplyTask.StartTime = time.Now()
		m.MapTaskStatus[reply.ReplyTask.Seq] = Running
		m.MapTaskStartTime[reply.ReplyTask.Seq] = reply.ReplyTask.StartTime
	} else {
		cntFinished := 0
		for i := 0; i < len(m.MapTaskStatus); i++ {
			if m.MapTaskStatus[i] == Finished {
				cntFinished += 1
			} else {
				break
			}
		}
		if cntFinished == m.MapTaskNum {
			m.MapTaskFinished = Finished
		}
	}
	reply.MapTaskFinished = m.MapTaskFinished
	DPrintf("end GetOneMapTask\n")
}

// RPC method. Worker request one reducetask. This function will give one reducetask
// to the Worker
func (m *Master) GetOneReduceTask(reply *RequestTaskReply) {
	DPrintf("begin GetOneReduceTask\n")
	reply.ReduceTaskNum = m.ReduceTaskNum
	reply.MapTaskNum = m.MapTaskNum
	reply.GotTask = false
	if m.ReduceTaskFinished == Finished {
		reply.ReduceTaskFinished = Finished
		return
	}
	// if there are avaliable tasks, then distribute one task to the worker
	if len(m.Tasks) > 0 {
		reply.ReplyTask = <-m.Tasks
		reply.ReplyTask.Status = Running
		reply.GotTask = true
		reply.ReplyTask.StartTime = time.Now()
		m.ReduceTaskStatus[reply.ReplyTask.Seq] = Running
		m.ReduceTaskStartTime[reply.ReplyTask.Seq] = reply.ReplyTask.StartTime
	} else {
		cntFinished := 0
		for i := 0; i < m.ReduceTaskNum; i++ {
			if m.ReduceTaskStatus[i] == Finished {
				cntFinished += 1
			} else {
				break
			}
		}
		if cntFinished == m.ReduceTaskNum {
			m.ReduceTaskFinished = Finished
		}
	}
	reply.ReduceTaskFinished = m.ReduceTaskFinished
	DPrintf("end GetOneReduceTask\n")
}

// Don't use this method
func (m *Master) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	return nil
}

//RequestTask is an RPC method that is called by workers to request a map or reduce task
func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	if args.TaskType == MapTask {
		m.GetOneMapTask(reply)
	} else {
		m.GetOneReduceTask(reply)
	}
	return nil
}

//ReportTask is an RPC method that is called by workers to report a task's status
//Only the task that finished in 10 seconds could call reportTask method.
func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	DPrintf("begin ReportTask\n")
	seq := args.ReportTask.Seq
	if args.ReportTask.TaskType == MapTask {
		DPrintf("begin map ReportTask\n")
		m.MapTaskStatus[seq] = args.ReportTask.Status
		DPrintf("end map ReportTask\n")
	} else {
		DPrintf("begin reduce ReportTask\n")
		m.ReduceTaskStatus[seq] = args.ReportTask.Status
		DPrintf("end reduce ReportTask\n")
	}
	DPrintf("end ReportTask\n")
	return nil
}

// Schedule all Map task to the task channel
func (m *Master) ScheduleMapTask() {
	DPrintf("begin ScheduleMapTask\n")
	// schedule tasks to worker process
	for i, status := range m.MapTaskStatus {
		switch status {
		case Unallocate:
			// schedule task
			task := Task{TaskType: MapTask, MapFile: m.Files[i], Status: Allocated, Seq: i}
			m.Tasks <- task
			m.MapTaskStatus[i] = Allocated
		default:
			DPrintf("default")
		}
	}
	DPrintf("end ScheduleMapTask\n")
}

// Schedule all Reduce task to the task channel
func (m *Master) ScheduleReduceTask() {
	DPrintf("begin ScheduleReduceTask\n")
	for i, status := range m.ReduceTaskStatus {
		switch status {
		case Unallocate:
			task := Task{TaskType: ReduceTask, Status: Allocated, Seq: i}
			m.Tasks <- task
			m.ReduceTaskStatus[i] = Allocated
		default:
			DPrintf("default")
		}
	}
	DPrintf("end ScheduleReduceTask\n")
}

// Schedule map tasks and reduce tasks to channel. Only after all map tasks finish, reduce tasks
// will begin schedule.
func (m *Master) ScheduleTask() {
	DPrintf("begin ScheduleTask\n")
	m.Mutex.Lock()
	m.ScheduleMapTask()
	m.Mutex.Unlock()
	for true {
		if m.MapTaskFinished == Finished {
			break
		}
		time.Sleep(time.Second)
	}
	m.Mutex.Lock()
	m.ScheduleReduceTask()
	m.Mutex.Unlock()
	DPrintf("end ScheduleTask\n")
}

// This method test if there are workers that don't finish theri work in 10 seconds.
// If there are some workers that don't finish on time, this method will put these tasks
// back to task channel.
func (m *Master) Monitor() {
	//test if there are map tasks failed
	for true {
		m.Mutex.Lock()
		if m.MapTaskFinished == Finished {
			m.Mutex.Unlock()
			break
		}
		for i := 0; i < m.MapTaskNum; i++ {
			if m.MapTaskStatus[i] == Running {
				duration := time.Now().Unix() - m.MapTaskStartTime[i].Unix()
				if duration > LongestDuration {
					task := Task{TaskType: MapTask, MapFile: m.Files[i], Status: Allocated, Seq: i}
					m.Tasks <- task
					m.MapTaskStatus[i] = Allocated
				}
			}
		}
		m.Mutex.Unlock()
		time.Sleep(time.Second)
	}

	//test if there are reduce tasks faild.
	for true {
		m.Mutex.Lock()
		if m.ReduceTaskFinished == Finished {
			m.Mutex.Unlock()
			break
		}
		for i := 0; i < m.ReduceTaskNum; i++ {
			if m.ReduceTaskStatus[i] == Running {
				duration := time.Now().Unix() - m.ReduceTaskStartTime[i].Unix()
				if duration > LongestDuration {
					task := Task{TaskType: ReduceTask, Status: Allocated, Seq: i}
					m.Tasks <- task
					m.ReduceTaskStatus[i] = Allocated
				}
			}
		}
		m.Mutex.Unlock()
		time.Sleep(time.Second)
	}
}

// Init all parameters of Master
func (m *Master) InitMaster(files []string, nReduce int) {
	// Your code here.
	// init master
	DPrintf("begin InitMaster\n")
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	m.MapTaskNum = len(files)
	m.ReduceTaskNum = nReduce
	m.MapTaskFinished = Unfinished
	m.ReduceTaskFinished = Unfinished
	m.Files = files
	m.Tasks = make(chan Task, m.MapTaskNum+nReduce)
	m.MapTaskStatus = make([]int, m.MapTaskNum)
	m.ReduceTaskStatus = make([]int, m.ReduceTaskNum)
	m.MapTaskStartTime = make([]time.Time, m.MapTaskNum)
	m.ReduceTaskStartTime = make([]time.Time, m.ReduceTaskNum)
	DPrintf("end InitMaster\n")
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	DPrintf("begin MakeMaster\n")
	m := Master{}
	m.InitMaster(files, nReduce)
	// start RPC server
	go m.server()
	// schedule tasks
	go m.ScheduleTask()
	// Monitor whether there are tasks that are not completed on time,
	// and put the tasks back into the channel
	go m.Monitor()
	return &m
}
