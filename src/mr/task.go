package mr

import (
	"fmt"
	"time"
)

type TaskStat int

const (
	TASK_READY int32 = iota
	TASK_RUNNING
	TASK_FINISH
	TASK_ERR
	TASK_LAST
)

type TaskType int

const (
	MAP TaskType = iota
	REDUCE
)

type Task struct {
	Id        int
	Type      TaskType
	State     int32
	Deadline  time.Duration
	FileNames []string
	NReduce   int
	WorkerId  int
}

func reduceFileName(mapId, reduceId int) string {
	return fmt.Sprintf("mr-%v-%v", mapId, reduceId)
}

func mergeFilename(reduceId int) string {
	return fmt.Sprintf("mr-out-%v", reduceId)
}
