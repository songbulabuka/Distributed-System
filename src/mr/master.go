package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "errors"
import "time"

type TaskType int
type TaskStatus int

const WaitTime=10

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
	Id int
	Filename string
	TaskType TaskType
	Status TaskStatus
	NReduce int
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
func (m *Master) GetTask(args *GetArgs, reply *GetReply) error{
	m.mu.Lock()
	fmt.Println("Get request from worker: ",args.Message)
	var this_task Task
	if m.map_num>0 {
		for i := 0; i < len(m.mapTasks); i++{
			if m.mapTasks[i].Status==Ready{
				m.mapTasks[i].Status = Process
				fmt.Printf("Handout map task, taskId:%v \n",i)
				this_task = m.mapTasks[i]
				break
			}
		}
	}else if m.reduce_num>0{
		for i := 0; i < len(m.reduceTasks); i++{
			if m.reduceTasks[i].Status==Ready{
				m.reduceTasks[i].Status = Process
				fmt.Printf("Handout reduce task, taskId:%v \n",i)
				this_task = m.reduceTasks[i]
				break
			}
		}
	}else{
		reply.Err = Err
	}
	reply.The_task = this_task
	go m.waitforTask(this_task.TaskType, this_task.Id)

	m.mu.Unlock()
	if reply.Err==""{
		return nil
	}else{
		return errors.New("no task remained")
	}
}


func (m *Master) waitforTask(taskType TaskType, id int) error{
	time.Sleep(time.Second*WaitTime)
	m.mu.Lock()
	if taskType==MapTask {
		m.mapTasks[id].Status = Ready
	}else if taskType==ReduceTask{
		m.reduceTasks[id].Status = Ready
	}else{
		return errors.New("error")
	}
	m.mu.Unlock()
	return nil
}

func (m *Master) PutTask(args *PutArgs, reply *PutReply) error{
	m.mu.Lock()
	fmt.Println("Get response from worker: ",args.Message)
	task := args.The_task
	fmt.Printf("Task: task Type %v, Filename: %v, task ID: %v\n", task.TaskType, task.Filename, task.Id)	
	if task.TaskType==MapTask{
		m.map_num--;
		m.mapTasks[task.Id].Status = Finished
	}else if task.TaskType==ReduceTask{
		m.reduce_num--;
		m.reduceTasks[task.Id].Status = Finished
	}else{
		reply.Err = "PutTast server error"
	}
	if m.map_num==0 && m.reduce_num==0{
		reply.Finish = true
	}
	reply.Message = "Copy, Got your message"
	m.mu.Unlock()
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
