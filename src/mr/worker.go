package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "path/filepath"
import "sort"
import "bufio"
//
// Map functions return a slice of KeyValue.
//
const RootPath = "/home/song/MIT6.824/6.824/src/main/"
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
		if task.TaskType==MapTask {
			map_func(task, mapf)
			putTask(task)
		}else if task.TaskType==ReduceTask{
			reduce_func(task, reducef)
			putTask(task)
		}else{
			fmt.Println("error")
		}
	}
	
	
}

func map_func(task Task, mapf func(string, string) []KeyValue){
	fmt.Println("worker now doing the map function!------")
	filename := RootPath+task.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		//return error
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		//return error
	}
	file.Close()
	kva := mapf(filename, string(content)) //[]mr.KeyValue
	map_writeFile(kva, task.Id, task.NReduce)
}

func map_writeFile(kva []KeyValue, taskId int, nReduce int){
	fmt.Println("  Map_writeFile------")
	files:=make([]*os.File, 0, nReduce)
	buffers := make([]*bufio.Writer, 0, nReduce)
	encs := make([]*json.Encoder, 0, nReduce)
	for i:=0;i<nReduce;i++{
		filename := fmt.Sprintf("%v/mr-%v-%v", RootPath, taskId, i) //RootPath+"/mr-"+strconv.Itoa(task.id)+"-"+strconv.Itoa(x)
		//file,err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		file, err := os.Create(filename)
		if err != nil {
			fmt.Println(err)
			fmt.Printf("Cannot create file %v\n", filename)
		}
		buf := bufio.NewWriter(file)
		buffers = append(buffers, buf)
		files = append(files, file)
		encs = append(encs, json.NewEncoder(file))
	}
	fmt.Println("  Writing to files")
	for _,kv := range kva {
		x := ihash(kv.Key) % nReduce
		if err := encs[x].Encode(&kv); err != nil {
			fmt.Printf("Can not encode %v to file\n",&kv)
			fmt.Println(err)
			break
		}
	}
	fmt.Println("  Map_writeFile Finished------")
}

func reduce_func(task Task, reducef func(string, []string) string){
	fmt.Println("Worker now doing the reduce function!------")
	files,err := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", RootPath, "*", task.Id))
	if err != nil {
		fmt.Println(err)
	}
	kva := make(map[string][]string)
	for _,file:=range files{
		jsonFile, err := os.Open(file)
		if err != nil {
			fmt.Println(err)
		}
		dec := json.NewDecoder(jsonFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva[kv.Key] = append(kva[kv.Key], kv.Value)
		}
	}
	reduce_writeFile(kva, task.Id, reducef)
}

func reduce_writeFile(kva map[string][]string, reduceID int, reducef func(string, []string) string){
	filePath:=fmt.Sprintf("%v/mr-out-%v", RootPath, reduceID)
	file,err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
	}
	// sort the map by key
	keys := make([]string, 0, len(kva))
	for k := range kva {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _,k:=range keys{
		output := reducef(k, kva[k])
		fmt.Fprintf(file, "%v %v\n", k, output)
	}

	file.Close()

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
	if ok{
		task := reply.The_task
		fmt.Printf("GetTask: Type %v, File: %v, ID: %v, Status:%v\n", task.TaskType, task.Filename, task.Id, task.Status)
		return task
	}else{
		var task Task
		fmt.Printf("reply.Err %v\n", reply.Err)
		return task
	}
}

func putTask(task Task){
	fmt.Println("PutTask------")
	// declare an argument structure.
	args := PutArgs{}
	// fill in the argument(s).
	args.Message = "Task Finished"
	args.The_task = task
	// declare a reply structure.
	reply := PutReply{}
	// send the RPC request, wait for the reply.
	fmt.Println("  Calling server---")
	ok:=call("Master.PutTask", &args, &reply)
	if ok{
		fmt.Printf("FinishedTask: task Type %v, Filename: %v, task ID: %v\n", task.TaskType, task.Filename, task.Id)	
	}else{
		fmt.Printf("reply.Err %v\n", reply.Err)
	}

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
