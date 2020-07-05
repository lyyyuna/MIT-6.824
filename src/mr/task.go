package mr

import "time"

type TaskStat int

const (
	TASK_READY TaskStat = iota
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
	State     TaskStat
	Deadline  time.Duration
	FileNames []string

	WorkerId int
}
