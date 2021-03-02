package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "fmt"
import "sync"
import "time"
import "strconv"

type Coordinator struct {
	mapJobs map[string]int64 // filename -> unix timestamp
	reduceJobs map[string]int64 // partition index -> unix timestamp
	lock sync.Mutex
	partitionCount int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) SendTask(args *SendTaskArgs, reply *SendTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	for fileName, startTime := range c.mapJobs {
		if time.Now().Unix() - startTime >= 10 {
			reply.FileName = fileName
			reply.TaskType = "Map"
			reply.PartitionCount = c.partitionCount
			c.mapJobs[fileName] = time.Now().Unix()
			return nil
		}
	}

	if len(c.mapJobs) > 0 { return nil }

	for index, startTime := range c.reduceJobs {
		if time.Now().Unix() - startTime >= 10 {
			reply.TaskType = "Reduce"
			i, err := strconv.Atoi(index)
			if err != nil {
				fmt.Println("welp shiet")
				return nil
			}
			reply.PartitionCount = i
			c.reduceJobs[index] = time.Now().Unix()
			return nil
		}
	}

	if len(c.reduceJobs) > 0 { return nil }

	// nothing left to do, we send the terminate signal to kill the worker
	reply.TaskType = "Terminate"

	return nil
}

func (c *Coordinator) SignalTaskDone(args *SignalTaskDoneArgs, reply *SignalTaskDoneReply) error {
	if args.TaskType == "Map" {
		c.lock.Lock()
		delete(c.mapJobs, args.FileName);
		c.lock.Unlock()
	} else if args.TaskType == "Reduce" {
		c.lock.Lock()
		delete(c.reduceJobs, strconv.Itoa(args.PartitionCount))
		c.lock.Unlock()
	} else {
		fmt.Println("hmm... getting a weird signal")
		fmt.Println(args.FileName)
		fmt.Println(args.TaskType)
	}
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
	c.lock.Lock()
	defer c.lock.Unlock()
	return len(c.reduceJobs) == 0
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{partitionCount: nReduce, mapJobs: make(map[string] int64), reduceJobs: make(map[string] int64)}

	// set the unix timestamp to 0, effectively saying the job has been running
	// forever and is ready to be picked up "again"
	for _, filename := range os.Args[1:] {
		c.mapJobs[filename] = 0
	}

	for i := 0; i < nReduce; i++ {
		c.reduceJobs[strconv.Itoa(i)] = 0
	}

	c.server()
	return &c
}
