package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

// Test if this file exist.
func FileExists(fileName string) bool {
	_, err := os.Stat(fileName)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

// Create a new file, if the original file exists, delete the original file first,
// and then create a new file
func CreateFile(fileName string) {
	if FileExists(fileName) {
		del := os.Remove(fileName)
		if del != nil {
			fmt.Println(del)
		}
	}
	f, err := os.Create(fileName)
	if err != nil {
		log.Fatalf("Cannot create file: ", fileName)
	}
	f.Close()

}

// Test Master one map task or one reduce task has finished on time
func ReportTask(task Task) {
	DPrintf("begin ReportTask\n")
	args := ReportTaskArgs{ReportTask: task}
	reply := ReportTaskReply{}
	call("Master.ReportTask", &args, &reply)
	DPrintf("end ReportTask\n")
}

// Worker does map task
func DoMapTask(mapf func(string, string) []KeyValue,
	reply RequestTaskReply) {
	DPrintf("begin DoMapTask\n")
	fileName := reply.ReplyTask.MapFile
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", file)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", file)
	}
	file.Close()
	kva := mapf(fileName, string(content))

	reduces := make([][]KeyValue, reply.ReduceTaskNum)
	for _, kv := range kva {
		idx := ihash(kv.Key) % reply.ReduceTaskNum
		reduces[idx] = append(reduces[idx], kv)
	}
	// write intermediate file
	seqMapTask := reply.ReplyTask.Seq
	for i, kvs := range reduces {
		//intermediateFileName := "mr-" + strconv.Itoa(seqMapTask) + "-" + strconv.Itoa(i)
		intermediateFileName := reduceName(seqMapTask, i)
		CreateFile(intermediateFileName)
		intermediateFile, err := os.OpenFile(intermediateFileName, os.O_RDWR|os.O_APPEND, os.ModeAppend)
		if err != nil {
			log.Fatalf("cannot open %v", intermediateFile)
		}
		enc := json.NewEncoder(intermediateFile)
		for _, kv := range kvs {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write to intermediate file : %v", err)
			}
		}
		if err := intermediateFile.Close(); err != nil {
			log.Fatalf("cannot close intermediate file", err)
		}
	}
	// if this task don't finish on time, this method will not tell master
	// that this task has finished.
	if time.Now().Unix()-reply.ReplyTask.StartTime.Unix() > LongestDuration {
		return
	}

	//After finish this task on time, then tell Master this task has finished on time.
	reply.ReplyTask.Status = Finished
	ReportTask(reply.ReplyTask)
	DPrintf("end DoMapTask\n")
}

// The Worker does recude task
func DoReduceTask(reducef func(string, []string) string, reply RequestTaskReply) {
	DPrintf("Begin DoReduceTask\n")
	intermediate := []KeyValue{}
	task := reply.ReplyTask
	// collect maps[word] = [1,1,1,1]
	for i := 0; i < reply.MapTaskNum; i++ {
		//filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(task.Seq)
		filename := reduceName(i, task.Seq)
		file, err := os.Open(filename)
		defer file.Close()
		//fmt.Println(filename)
		if err != nil {
			continue
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				// DPrintf("Decode Erro: ", err)
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	seqReduceTask := task.Seq
	//oname := "mr-out-" + strconv.Itoa(seqReduceTask)
	oname := mergeName(seqReduceTask)
	CreateFile(oname)
	f, err := os.OpenFile(oname, os.O_RDWR|os.O_APPEND, os.ModeAppend)
	if err != nil {
		log.Fatalf("cannot open %v", oname)
	}

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
		fmt.Fprintf(f, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	f.Close()

	// if this task don't finish on time, this method will not tell master
	// that this task has finished.
	if time.Now().Unix()-reply.ReplyTask.StartTime.Unix() > LongestDuration {
		return
	}

	//After finish this task on time, then tell Master this task has finished on time.
	reply.ReplyTask.Status = Finished
	ReportTask(reply.ReplyTask)
	DPrintf("end DoReduceTask\n")
}

// mrworker.go will call this method to start work process
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	// Map Phase
	DPrintf("begin Worker\n")
	for true {
		args := RequestTaskArgs{TaskType: MapTask}
		reply := RequestTaskReply{}
		res := call("Master.RequestTask", &args, &reply)
		if !res {
			break
		}
		if reply.MapTaskFinished == Finished {
			break
		}

		if reply.GotTask {
			DoMapTask(mapf, reply)
		} else {
			// When there is no task to do, just wait a second
			time.Sleep(time.Millisecond * 100)
		}
	}

	// Reduce Phase
	for true {
		args := RequestTaskArgs{TaskType: ReduceTask}
		reply := RequestTaskReply{}
		res := call("Master.RequestTask", &args, &reply)
		if !res {
			break
		}
		if reply.ReduceTaskFinished == Finished {
			break
		}

		if reply.GotTask {
			DoReduceTask(reducef, reply)
		} else {
			// When there is no task to do, just wait a second
			time.Sleep(time.Millisecond * 100)
		}
	}
	DPrintf("end Worker")
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//socketname := masterSock()
	socketname := "mr-socket"
	c, err := rpc.DialHTTP("unix", socketname)
	if err != nil {
		DPrintf("rpcname: %v\n", rpcname)
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	return false
}
