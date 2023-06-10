package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	maptask    = "map"
	reducetask = "reduce"
)

const (
	created  = "created"
	finished = "finished"
)

const (
	mapcondition    = "working on map"
	reductcondition = "working on reduce"
	finishcondition = "finished"
)

type Coordinator struct {
	// Your definitions here.
	Taskchan       chan Task
	Reducechan     chan Task
	NReduce        int // number of reduce tasks
	NMap           int // number of map tasks
	Condition      string
	RuMutex        sync.Mutex
	TaskMap        map[int]Task
	FinishedMap    int
	FinishedReduce int
	Phase          string
}

type Task struct {
	// Your definitions here.
	Id                    int      // task id
	TaskType              string   // map or reduce
	File                  string   // file name
	Status                string   // idle, in progress, completed
	NReduce               int      // number of reduce tasks
	NMap                  int      // number of map tasks
	IntermediateFilenames []string // intermediate file names
	StartTime             time.Time
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.RuMutex.Lock()
	defer c.RuMutex.Unlock()
	if c.Condition == mapcondition {
		task := <-c.Taskchan
		reply.Task = task
		if task.Status == created {
			c.TaskMap[task.Id] = task
		}
		if c.FinishedMap == c.NMap {
			c.Condition = reductcondition
		}
	} else if c.Condition == reductcondition {
		task := <-c.Reducechan
		reply.Task = task
		if task.Status == created {
			c.TaskMap[task.Id] = task
		}
	} else {
		return nil
	}
	return nil
}

func (c *Coordinator) UpdateTaskStatus(args *UpdateTaskStatusArgs, reply *UpdateTaskStatusReply) error {
	c.RuMutex.Lock()
	defer c.RuMutex.Unlock()
	index := args.Id
	task := c.TaskMap[index]
	if task.Status == created {
		task.Status = finished
		if task.TaskType == maptask {
			c.FinishedMap++
		} else {
			c.FinishedReduce++
		}
	}
	c.TaskMap[index] = task
	//fmt.Printf("Number of finished map tasks: %v\n", c.FinishedMap)
	//fmt.Printf("Number of finished reduce tasks: %v\n", c.FinishedReduce)
	if c.FinishedMap == c.NMap {
		fmt.Printf("All map tasks are finished\n")
		c.Condition = reductcondition
	}
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	c.RuMutex.Lock()
	defer c.RuMutex.Unlock()
	if c.FinishedMap == c.NMap && c.FinishedReduce == c.NReduce {
		ret = true
		c.Condition = finishcondition
	}
	return ret
}

func (c *Coordinator) IsDone(args *GetConditionArgs, reply *GetConditionReply) error {
	reply.Done = c.Done()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) catchTimeout() {
	for {
		time.Sleep(5 * time.Second)
		c.RuMutex.Lock()
		defer c.RuMutex.Lock()
		if c.Condition != finishcondition {
			for i, task := range c.TaskMap {
				if task.Status == created {
					// check timeout
					if time.Now().Sub(task.StartTime) > 10*time.Second {
						fmt.Printf("task %v timeout\n", i)
						if task.TaskType == maptask {
							c.Taskchan <- task
						} else {
							c.Reducechan <- task
						}
					}
				}
			}
		} else {
			return
		}

	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Taskchan:       make(chan Task),
		Reducechan:     make(chan Task),
		NReduce:        nReduce,
		NMap:           len(files),
		Condition:      mapcondition,
		TaskMap:        make(map[int]Task),
		FinishedMap:    0,
		FinishedReduce: 0,
	}

	c.server()

	// Your code here.
	for i, file := range files {
		//fmt.Printf("coordinator working on file: %v\n", file)
		task := Task{
			Id:        i,
			TaskType:  maptask,
			File:      file,
			Status:    created,
			NReduce:   nReduce,
			NMap:      len(files),
			StartTime: time.Now(),
		}
		//fmt.Printf("generate task: %v\n", task)
		c.Taskchan <- task
		c.TaskMap[i] = task
		//fmt.Printf("coordinator send task: %v\n", task)
	}
	//fmt.Printf("Coordinator start %v map tasks", c.NMap)

	//fmt.Printf("Coordinator start %v reduce tasks", c.NReduce)

	for i := 0; i < c.NReduce; i++ {
		task := Task{
			Id:        i + c.NMap,
			TaskType:  reducetask,
			Status:    created,
			NReduce:   c.NReduce,
			NMap:      c.NMap,
			StartTime: time.Now(),
		}
		IntermediateFilenames := make([]string, c.NMap)
		for j := 0; j < c.NMap; j++ {
			IntermediateFilenames[j] = fmt.Sprintf("mr-%v-%v", j, i)
		}
		task.IntermediateFilenames = IntermediateFilenames
		//fmt.Printf("generate reduce task: %v\n", task)
		c.Reducechan <- task
		c.TaskMap[i+c.NMap] = task
		//fmt.Printf("coordinator send reduce task: %v\n", task)
	}

	go c.catchTimeout()

	return &c
}
