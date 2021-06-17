package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"


type Task struct {
	inputs []string
	status int  // 0 for initial state, 1 for running, 2 for finished
	outputs []string
}

type Coordinator struct {
	// Your definitions here.
	mapTasks []Task
	reduceTasks []Task
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) WorkHandler(args *AskForWork, reply *Work) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	canReduce := true
	for id, task := range c.mapTasks {
		if task.status == 0 {
			reply.workType = 0
			reply.inputs = task.inputs
			reply.id = id
			reply.nReduce = len(c.reduceTasks)
			task.status = 1
			return nil
		}
		if task.status == 1 {
			canReduce = false
		}
	}

	if canReduce {
		for id, task := range c.reduceTasks {
			if task.status == 0 {
				reply.workType = 0
				reply.inputs = task.inputs
				reply.id = id
				reply.nReduce = len(c.reduceTasks)
				task.status = 1
				return nil
			}
		}
	}

	reply.workType = -1
	return nil
}

func (c *Coordinator) ResultHandler(args *HandoverWork, reply *HandoverAck) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.workType {
	case 0:
		c.mapTasks[args.id].outputs = args.outputs
		c.mapTasks[args.id].status = 2
	case 1:
		c.reduceTasks[args.id].outputs = args.outputs
		c.reduceTasks[args.id].status = 2
	}

	reply.received = true
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
	for _, task := range c.reduceTasks {
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
	c.mu.Lock()
	for i, file := range files {
		task := Task{
			inputs: []string{file},
			status: 0,
			outputs: make([]string, nReduce),
		}
		c.mapTasks[i] = task
	}
	for i := 0; i < nReduce; i++ {
		task := Task{
			inputs: make([]string, len(files)),
			status: 0,
			outputs: make([]string, 1),
		}
		c.reduceTasks[i] = task
	}
	c.mu.Unlock()


	c.server()
	return &c
}
