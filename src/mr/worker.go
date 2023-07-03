package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "strconv"
import "encoding/json"
import "path/filepath"
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
		if task.taskType==MapTask {
			map_func(task, mapf)
			putTask(task)
		}else if task.taskType==ReduceTask{
			reduce_func(task, reducef)
			putTask(task)
		}else{
			fmt.Println("error")
		}
	}
	
	
}

func map_func(task Task, mapf func(string, string) []KeyValue){
	fmt.Println("worker now doing the map function!")
	filename := RootPath+task.filename
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
	map_writeFile(kva,task.id)
}

func map_writeFile(kva []mr.KeyValue,taskId int){
	for _,kv := range kva {
		x := ihash(kv.Key) % task.nReduce
		kv_json, err := json.Marshal(kv)
		if err != nil {
			fmt.Println(err)
		}
		inter_filename := fmt.Sprintf("%v/mr-%v-%v", RootPath, task.id, x) //RootPath+"/mr-"+strconv.Itoa(task.id)+"-"+strconv.Itoa(x)
		_,err = os.OpenFile(inter_filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Println(err)
		}
		err = ioutil.WriteFile(inter_filename, kv_json, 0644)  // append?
		if err != nil {
			log.Fatal(err)
		}
	}
}

func reduce_func(task Task, reducef func(string, []string) string){
	fmt.Println("Worker now doing the reduce function!")
	files,err := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", RootPath, "*", task.id))
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
	reduce_writeFile(kva, task.id)
}

func reduce_writeFile(kva map[string][]string, reduceID int){
	filePath:=fmt.Sprintf("%v/mr-out-%v", RootPath, "*", task.id)
	file,err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
	}
	enc := json.NewEncoder(file)
	for _,kv := range kva {
		err := enc.Encode(&kv)


	}

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

func putTask(task Task){
	// declare an argument structure.
	args := PutArgs{}
	// fill in the argument(s).
	args.Message = "Ask for a task."
	// declare a reply structure.
	reply := PutReply{}
	// send the RPC request, wait for the reply.
	ok:=call("Master.PutTask", &args, &reply)

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
