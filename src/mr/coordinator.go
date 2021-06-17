package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "fmt"
import "time"


type Task struct {
	inputs []string
	status int  // 0 for initial state, 1 for running, 2 for finished
	outputs []string
	timestamp time.Time  // timestamp of the status
}

type Coordinator struct {
	// Your definitions here.
	mapTasks []Task
	reduceTasks []Task
}
var mu = sync.Mutex{}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) WorkHandler(args *AskForWork, reply *Work) error {
	mu.Lock()
	defer mu.Unlock()

	fmt.Println("Received work request")
	fmt.Println("Current coordinator state: ", c)
	canReduce := true
	for id, task := range c.mapTasks {
		if task.status == 0 {
			reply.WorkType = 0
			reply.Inputs = task.inputs
			reply.Id = id
			reply.NReduce = len(c.reduceTasks)
			c.mapTasks[id].status = 1
			c.mapTasks[id].timestamp = time.Now()
			return nil
		}
		if task.status == 1 {
			canReduce = false
		}
	}

	if canReduce {
		for id, task := range c.reduceTasks {
			if task.status == 0 {
				reply.WorkType = 1
				reply.Inputs = task.inputs
				reply.Id = id
				reply.NReduce = len(c.reduceTasks)
				c.reduceTasks[id].status = 1
				c.reduceTasks[id].timestamp = time.Now()
				return nil
			}
		}
	}

	reply.WorkType = -1
	return nil
}

func (c *Coordinator) ResultHandler(args *HandoverWork, reply *HandoverAck) error {
	fmt.Println("Received result ", args)
	mu.Lock()
	defer mu.Unlock()

	switch args.WorkType {
	case 0:
		c.mapTasks[args.Id].outputs = args.Outputs
		c.mapTasks[args.Id].status = 2
		for id, filename := range args.Outputs {
			c.reduceTasks[id].inputs = append(c.reduceTasks[id].inputs, filename)
		}
	case 1:
		c.reduceTasks[args.Id].outputs = args.Outputs
		c.reduceTasks[args.Id].status = 2
	}

	reply.Received = true
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.
	mu.Lock()
	defer mu.Unlock()
	timeout, _ := time.ParseDuration("10s")
	current := time.Now()
	for id, task := range c.mapTasks {
		if task.status == 1 && current.Sub(task.timestamp) > timeout {
			c.mapTasks[id].status = 0
		}
	}
	for id, task := range c.reduceTasks {
		if task.status == 1 && current.Sub(task.timestamp) > timeout {
			c.reduceTasks[id].status = 0
		}
		if task.status != 2 {
			ret = false
		}
	}


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks: make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
	}

	// Your code here.
	for i, file := range files {
		task := Task{
			inputs: []string{file},
			status: 0,
			outputs: make([]string, 0),
		}
		c.mapTasks[i] = task
	}
	for i := 0; i < nReduce; i++ {
		task := Task{
			inputs: make([]string, 0),
			status: 0,
			outputs: make([]string, 0),
		}
		c.reduceTasks[i] = task
	}
	fmt.Println("Initialized coordinator: ", c)


	c.server()
	return &c
}
