package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MASTER_INIT int32 = iota
	MASTER_MAP_FINISHED
	MASTER_REDUCE_FINISHED
)

//
// intermediateFiles
//              列0            列1           列2
// 行0-reduce-0 [map1-reduce0，map2-reduce0，map3-reduce0......]
// 行1-reduce-1 [map1-reduce1，map2-reduce1，map3-reduce1......]
// 行2-reduce-2 [map1-reduce2，map2-reduce2，map3-reduce2......]
type Master struct {
	// Your definitions here.
	files             []string
	state             int32
	mapTasks          []*Task
	reduceTasks       []*Task
	intermediateFiles [][]string
	nReduce           int32
	nMap              int32
	nCompleteReduce   int32
	nCompleteMap      int32

	mapTaskChan    chan *Task
	reduceTaskChan chan *Task
	mu             sync.RWMutex
	workerCount    int
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
	m.mu.Lock()
	defer m.mu.Unlock()
	if atomic.LoadInt32(&m.state) == MASTER_REDUCE_FINISHED {
		return true
	} else {
		return false
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files:           files,
		state:           MASTER_INIT,
		nReduce:         int32(nReduce),
		nMap:            int32(len(files)),
		nCompleteReduce: 0,
		nCompleteMap:    0,
		mapTasks:        make([]*Task, len(files)),
		reduceTasks:     make([]*Task, nReduce),
		workerCount:     0,
		mapTaskChan:     make(chan *Task, len(files)),
		reduceTaskChan:  make(chan *Task, nReduce),
	}
	// Your code here.

	m.server()
	m.generateMapTasks()
	DPrintf("master init")
	return &m
}

func (m *Master) generateMapTasks() {
	for i, file := range m.files {
		mapTask := &Task{
			Id:        i,
			Type:      MAP,
			State:     TASK_READY,
			Deadline:  10 * time.Second,
			FileNames: []string{file},
			NReduce:   int(m.nReduce),
		}
		m.mapTasks[i] = mapTask
		m.mapTaskChan <- mapTask
	}
}

func (m *Master) generateReduceTasks() {
	for i := 0; i < int(m.nReduce); i++ {
		reduceTask := &Task{
			Id:        i,
			Type:      REDUCE,
			State:     TASK_READY,
			Deadline:  10 * time.Second,
			FileNames: make([]string, 0),
			NReduce:   int(m.nReduce),
		}
		for j := 0; j < int(m.nMap); j++ {
			reduceTask.FileNames = append(reduceTask.FileNames, reduceFileName(j, i))
		}

		m.reduceTasks[i] = reduceTask
		m.reduceTaskChan <- reduceTask
	}
}

func (m *Master) GetOneTask(args *PullTaskArg, reply *PullTaskReply) error {
	select {
	case mapTask := <-m.mapTaskChan:
		sendTask := Task{
			Id:        mapTask.Id,
			FileNames: mapTask.FileNames,
			Type:      mapTask.Type,
			NReduce:   mapTask.NReduce,
			State:     atomic.LoadInt32(&mapTask.State),
		}
		reply.Task = &sendTask
		go m.monitorTask(mapTask)
		DPrintf("assign one map task: %v, filename: %v", sendTask.Id, sendTask.FileNames[0])
	case reduceTask := <-m.reduceTaskChan:
		sendTask := Task{
			Id:        reduceTask.Id,
			FileNames: reduceTask.FileNames,
			Type:      reduceTask.Type,
			NReduce:   reduceTask.NReduce,
			State:     atomic.LoadInt32(&reduceTask.State),
		}
		reply.Task = &sendTask
		go m.monitorTask(reduceTask)
		DPrintf("assign one reduce task: %v, filename: %v", sendTask.Id, sendTask.FileNames)
	}
	return nil
}

func (m *Master) monitorTask(t *Task) {
	timer := time.NewTicker(t.Deadline)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			atomic.StoreInt32(&t.State, TASK_READY)
			DPrintf("ERROR, one task failed: %v", t)
			if t.Type == MAP {
				m.mapTaskChan <- t
			} else {
				m.reduceTaskChan <- t
			}

		default:
			time.Sleep(100 * time.Millisecond)
			// if t.Type == MAP {
			// 	if m.mapTasks[t.Id].State == TASK_FINISH {
			// 		return
			// 	}
			// } else {
			// 	if m.reduceTasks[t.Id].State == TASK_FINISH {
			// 		return
			// 	}
			// }
			taskState := atomic.LoadInt32(&t.State)
			if taskState == TASK_FINISH {
				return
			}
		}
	}
}

func (m *Master) FinishTask(args *ReportTaskArg, reply *ReportTaskReply) error {
	switch args.Task.Type {
	case MAP:
		DPrintf("finish one map task: %v, filename: %v", args.Task.Id, args.Task.FileNames[0])
		if args.Task.State == TASK_ERR {
			DPrintf("master receive error task, resceduling")
			m.mapTaskChan <- m.mapTasks[args.Task.Id]
		}
		atomic.StoreInt32(&m.mapTasks[args.Task.Id].State, TASK_FINISH)
		atomic.AddInt32(&m.nCompleteMap, 1)

		if atomic.LoadInt32(&m.nCompleteMap) == m.nMap {
			DPrintf("all map tasks finished")
			atomic.StoreInt32(&m.state, MASTER_MAP_FINISHED)
			go m.generateReduceTasks()
		}
	case REDUCE:
		DPrintf("finish one reduce task: %v, filename: %v", args.Task.Id, args.Task.FileNames)
		if args.Task.State == TASK_ERR {
			DPrintf("master receive error task, resceduling")
			m.reduceTaskChan <- m.reduceTasks[args.Task.Id]
		}
		atomic.StoreInt32(&m.reduceTasks[args.Task.Id].State, TASK_FINISH)
		atomic.AddInt32(&m.nCompleteReduce, 1)

		if atomic.LoadInt32(&m.nCompleteReduce) == m.nReduce {
			DPrintf("all reduce tasks finished")
			atomic.StoreInt32(&m.state, MASTER_REDUCE_FINISHED)
		}

	}

	return nil
}

func (m *Master) RegisterWorker(args *RegisterWorkerArg, reply *RegisterWorkerReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	reply.Id = m.workerCount
	m.workerCount += 1

	return nil
}
