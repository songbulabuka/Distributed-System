package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
// import "os"
// import "io/ioutil"

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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	fmt.Println("Making Worker------")
	for{
		task:=getTask()
		if task.taskType==MapTask {
			map_func()
		}else if task.taskType==ReduceTask{
			reduce_func()
		}else{
			fmt.Println("error")
		}
	}
	
	
}

func map_func(){
	fmt.Println("worker now doing the map function!")
}

func reduce_func(){
	fmt.Println("Worker now doing the reduce function!")
}


//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func getTask() Task{

	// declare an argument structure.
	args := GetArgs{}

	// fill in the argument(s).
	args.Message = "Ask for a task."

	// declare a reply structure.
	reply := GetReply{}

	// send the RPC request, wait for the reply.
	ok:=call("Master.GetTask", &args, &reply)
	task := reply.The_task
	if ok{
		// reply.Y should be 100.
		fmt.Printf("task Type %v\n, filename %v\n", task.taskType, task.filename)
	}else{
		fmt.Printf("reply.Err %v\n", reply.Err)
	}
	return task
	
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
