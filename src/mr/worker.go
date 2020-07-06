package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
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

type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	w := worker{
		mapf:    mapf,
		reducef: reducef,
	}
	w.register()
	w.run()
}

// register self to Master
func (w *worker) register() {
	args := &RegisterWorkerArg{}
	reply := &RegisterWorkerReply{}
	if ok := call("Master.RegisterWorker", args, reply); !ok {
		DPrintf("register failed")
	}
	w.id = reply.Id
	DPrintf("worker %v register", w.id)
}

func (w *worker) run() {
	for {
		t := w.gettask()
		w.dotask(t)
	}
}

func (w *worker) gettask() *Task {
	args := &PullTaskArg{}
	reply := &PullTaskReply{}

	if ok := call("Master.GetOneTask", args, reply); !ok {
		DPrintf("get one task failed")
		DPrintf("worker %v exited", w.id)
		os.Exit(1)
	}
	DPrintf("get one task: %v", reply.Task)
	return reply.Task
}

func (w *worker) dotask(t *Task) {
	switch t.Type {
	case MAP:
		w.doMapTask(t)
	case REDUCE:
		w.doReduceTask(t)
	default:
		panic("unknown task type")
	}
}

func (w *worker) reportTask(t *Task, err error) {
	args := &ReportTaskArg{
		Task: t,
	}
	if err != nil {
		args.Task.State = TASK_ERR
	}
	reply := &ReportTaskReply{}
	if ok := call("Master.FinishTask", args, reply); !ok {
		DPrintf("report task failed, type: %v, task: %v", t.Type, t.Id)
	}

}

func (w *worker) doMapTask(t *Task) {
	contents, err := ioutil.ReadFile(t.FileNames[0])
	if err != nil {
		log.Printf("read file error: %v", err)
		w.reportTask(t, err)
		return
	}

	kvs := w.mapf(t.FileNames[0], string(contents))
	reduces := make([][]KeyValue, t.NReduce)

	for _, kv := range kvs {
		idx := ihash(kv.Key) % t.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}

	for reduceIndex, content := range reduces {
		filename := reduceFileName(t.Id, reduceIndex)
		f, err := os.Create(filename)
		defer f.Close()
		if err != nil {
			log.Printf("create file error: %v", err)
			w.reportTask(t, err)
			return
		}

		enc := json.NewEncoder(f)
		for _, kv := range content {
			if err := enc.Encode(&kv); err != nil {
				w.reportTask(t, err)
				return
			}
		}
	}

	w.reportTask(t, nil)
}

//
// intermediateFiles
//              列0            列1           列2
// 行0-reduce-0 [map1-reduce0，map2-reduce0，map3-reduce0......]
// 行1-reduce-1 [map1-reduce1，map2-reduce1，map3-reduce1......]
// 行2-reduce-2 [map1-reduce2，map2-reduce2，map3-reduce2......]
func (w *worker) doReduceTask(t *Task) {
	mergeKeys := make(map[string][]string)
	for _, itermediateFile := range t.FileNames {
		f, err := os.Open(itermediateFile)
		if err != nil {
			DPrintf("fail to open itermediateFile: %v, error: %v", itermediateFile, err)
			w.reportTask(t, err)
		}
		defer f.Close()

		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				// DPrintf("decode error: %v", err)
				// EOF
				break
			}

			if _, ok := mergeKeys[kv.Key]; !ok {
				mergeKeys[kv.Key] = make([]string, 0)
			}
			mergeKeys[kv.Key] = append(mergeKeys[kv.Key], kv.Value)
		}
	}

	mergeFileName := mergeFilename(t.Id)
	res := make([]string, 0)
	for k, v := range mergeKeys {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}
	if err := ioutil.WriteFile(mergeFileName, []byte(strings.Join(res, "")), 0600); err != nil {
		w.reportTask(t, err)
		return
	}

	w.reportTask(t, nil)
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
