package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "strings"
import "os"
import "bufio"
import "io"
import "io/ioutil"
import "strconv"
import "time"


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

// Readln returns a single line (without the ending \n)
// from the input buffered reader.
// An error is returned iff there is an error with the
// buffered reader.
func readLine(r *bufio.Reader) (string, error) {
	var (isPrefix bool = true
			 err error = nil
			 line, ln []byte
			)
	for isPrefix && err == nil {
			line, isPrefix, err = r.ReadLine()
			ln = append(ln, line...)
	}
	return string(ln),err
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		reply := CallSendTask()
		if reply.TaskType == "Map" {
			doMapWork(mapf, reply)
			CallSignalTaskDone(reply.FileName, reply.TaskType, reply.PartitionCount)
		} else if reply.TaskType == "Reduce" {
			doReduceWork(reducef, reply)
			CallSignalTaskDone(reply.FileName, reply.TaskType, reply.PartitionCount)
		} else if reply.TaskType == "Terminate" {
			return
		}
		time.Sleep(100)
	}
}

func doMapWork(mapf func(string, string) []KeyValue, reply SendTaskReply) {
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
	}

	kva := mapf(reply.FileName, string(content))

	// open n files where n is partitionCount sent from coordinator
	intermediateFiles := []*os.File{}
	for i :=0; i < reply.PartitionCount; i++ {
		oname := "mr-intermediate-" + strconv.Itoa(i) + "-" + getFileNameOnly(reply.FileName)
		ofile, _ := os.Create(oname)
		intermediateFiles = append(intermediateFiles, ofile)
	}

	for _, kv := range kva {
		// for each word, hash the word, modulo partitionCount and append in file
		fmt.Fprintf(intermediateFiles[ihash(kv.Key) % reply.PartitionCount], "%v %v\n", kv.Key, kv.Value)
	}

	// close all the opened files
	for i :=0; i < reply.PartitionCount; i++ {
		intermediateFiles[i].Close()
	}
}

func doReduceWork(reducef func(string, []string) string, reply SendTaskReply) {
	prefix := "mr-intermediate-" + strconv.Itoa(reply.PartitionCount)
	resultMap := make(map[string][]string)

	// loop over files and find matches of mr-intermediate-{partitionIndex}
	files, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}

	for _, fileInfo := range files {
		// example intermediate filenames: mr-intermediate-2-pg-tom_sawyer.txt
		// here we loop through the files under the partitionCount
		if !strings.HasPrefix(fileInfo.Name(), prefix) { continue }
		file, err := os.Open(fileInfo.Name())
		if err != nil {
			log.Fatalf("cannot open %v", fileInfo.Name())
		}
		reader := bufio.NewReader(file)
		for {
			line, err := readLine(reader)
			if err == io.EOF { break }

			lineContent := strings.Split(line, " ")
			k := lineContent[0]
			v := lineContent[1]

			if _, ok := resultMap[k]; ok {
				resultMap[k] = append(resultMap[k], v)
			} else {
				resultMap[k] = []string{v}
			}
		}
	}

	finalFileName := "mr-out-" + strconv.Itoa(reply.PartitionCount)
	ofile, _ := os.Create(finalFileName)
	for k, kva := range resultMap {
		fmt.Fprintf(ofile, "%v %v\n", k, reducef(k, kva))
	}
	ofile.Close()
}

func getFileNameOnly(path string) string {
	temp := strings.Split(path, "/")
	return temp[len(temp)-1]
}

func CallSendTask() SendTaskReply {
	args := SendTaskArgs{}
	reply := SendTaskReply{}
	call("Coordinator.SendTask", &args, &reply)
	return reply
}

func CallSignalTaskDone(fileName string, taskType string, partitionCount int) {
	args := SignalTaskDoneArgs{FileName: fileName, TaskType: taskType, PartitionCount: partitionCount }
	reply := SignalTaskDoneReply{}
	call("Coordinator.SignalTaskDone", &args, &reply)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	fmt.Printf("something wrong with rpc call... error=%s\n", err)
	return false
}
