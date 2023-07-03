package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type TaskType int
type TaskStatus int

const (
	OK  = "OK"
	Err = "Err"
)

const (
	MapTask TaskType=iota  // https://yourbasic.org/golang/iota/
	ReduceTask
)

const (
	Ready TaskStatus=iota
	Process
	Finished
	Fail
)

type Task struct{
	id int
	filename string
	taskType TaskType
	status TaskStatus
	nReduce int
}

type Master struct {
	// Your definitions here.
	mu sync.Mutex
	mapTasks []Task
	reduceTasks []Task
	map_num int
	reduce_num int
}

// // Your code here -- RPC handlers for the worker to call.

// //
// // an example RPC handler.
// //
// // the RPC argument and reply types are defined in rpc.go.
// //
// func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

func (m *Master) GetTask(args *GetArgs, reply *GetReply) error{
	m.mu.Lock()
	defer m.mu.Unlock()
	fmt.Println("Get request from worker: ",args)
	if m.map_num!=0 {
		for _,task := range m.mapTasks{
			if task.status==Ready{
				reply.The_task = task
				reply.Filename = task.filename
				return nil
			}
		}
	}

	if m.reduce_num!=0{
		for _,task := range m.reduceTasks{
			if task.filename!="" && task.status==Ready{
				reply.The_task = task
				reply.Filename = task.filename
				return nil
			}
		}
	}

	reply.Err = Err
	return nil
}

func (m *Master) PutTask(args *PutArgs, reply *PutReply) error{
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	// Your code here.
	if m.map_num==0 && m.reduce_num==0{
		ret = true
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.
	m_tasks:=make([]Task, len(files)) // len()=0, cap()=files length
	r_tasks:=make([]Task, nReduce)

	for i:= range m_tasks{
		m_tasks[i] = Task{i, files[i], MapTask, Ready, nReduce}
	}
	for i:= range r_tasks{
		r_tasks[i] = Task{i, "", ReduceTask, Ready, nReduce}
	}

	m.mapTasks=m_tasks
	m.reduceTasks=r_tasks
	m.map_num = len(files)
	m.reduce_num = nReduce

	m.server()
	fmt.Println("Making server------")
	return &m
}
