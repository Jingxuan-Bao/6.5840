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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Done() bool {
	time.Sleep(500 * time.Millisecond)
	args := GetConditionArgs{}
	reply := GetConditionReply{}
	ok := call("Coordinator.IsDone", &args, &reply)
	if !ok {
		log.Fatal("call Coordinator.IsDone failed")
	}
	return reply.Done
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for !Done() {
		task := GetTask()
		//fmt.Printf("GetTask: %v\n", task)
		switch task.TaskType {
		case "map":
			DoMap(task, mapf)
		case "reduce":
			DoReduce(task, reducef)
		}
	}
}

func GetTask() Task {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		log.Fatal("call Coordinator.GetTask failed")
	} else {
		//fmt.Printf("GetTask: %v\n", reply.Task)
	}
	return reply.Task
}

func DoMap(task Task, mapf func(string, string) []KeyValue) {
	intermediate := []KeyValue{}
	filename := task.File
	//fmt.Printf("DoMap: %v\n", filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	//fmt.Printf("DoMap: %v\n", kva)
	intermediate = append(intermediate, kva...)
	//fmt.Printf("DoMap: %v\n", intermediate)
	fielEncoder := make(map[string]*json.Encoder, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		filename := fmt.Sprintf("mr-%v-%v", task.Id, i)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("cannot create %v", filename)
		}
		fielEncoder[filename] = json.NewEncoder(file)
	}
	for _, kv := range intermediate {
		filename := fmt.Sprintf("mr-%v-%v", task.Id, ihash(kv.Key)%task.NReduce)
		if err := fielEncoder[filename].Encode(&kv); err != nil {
			log.Fatalf("cannot encode %v", kv)
			return
		}
	}
	UpdateTaskStatus(task.Id, "map")
}

func DoReduce(task Task, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for _, interintermediateFilename := range task.IntermediateFilenames {
		file, err := os.Open(interintermediateFilename)
		if err != nil {
			log.Fatalf("cannot open %v", interintermediateFilename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	outputname := fmt.Sprintf("mr-out-%v", task.Id-task.NMap)
	outfile, err := os.Create(outputname)

	if err != nil {
		log.Fatalf("cannot create %v", outputname)
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
		fmt.Fprintf(outfile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	outfile.Close()
	UpdateTaskStatus(task.Id, "reduce")
}

func UpdateTaskStatus(taskId int, taskType string) {
	args := UpdateTaskStatusArgs{taskId, taskType}
	reply := UpdateTaskStatusReply{}
	ok := call("Coordinator.UpdateTaskStatus", &args, &reply)
	if !ok {
		log.Fatal("call Coordinator.UpdateTaskStatus failed")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
