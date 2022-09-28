package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "strings"
import "fmt"
import "sync"
import "time"
import "strconv"


const (
	Sleep=iota
	Map
	Reduce
	Finish
)
const (
	Working=iota
	Timeout
)
const (
	NotStarted=iota
	Processing
	Finished
)


type MapInput struct{
	Done int
	Data string
}

type Coordinator struct {
	// Your definitions here.

	// Num of Worker for reduce proess
	NumReduceWorker int

	// Num of input file part
	NumMapFile int


	//State 0 sleep, 1 map, 2 reduce, 3 finish 
	State int

	//map任务产生的中间件存储 
	ReduceRecord map[int]string

	//List for record which file is busy
	Mapfiles map[string]int
	Reducefiles map[string]int

	// map result file name
	intermediafiles []string
	Mux sync.Mutex

	//记录现有map任务的数量
	MapTaskNum int


}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) GetTask(args *GetTaskRequest, reply *GetTaskReply) error {
	c.Mux.Lock()




	defer c.Mux.Unlock()
	if c.State == Sleep{
		reply.TaskType = Sleep
		return nil
	}else if c.State == Finish{
		reply.TaskType = Finish
		return nil
	}else if c.State == Map{
		reply.TaskType = Map
		//给worker一个编号
		if args.Index == -1{
			reply.Index = c.MapTaskNum
			c.MapTaskNum++
		}
		// map 的逻辑，查看哪个文件是空闲的，分发给worker，
		for k,v := range c.Mapfiles{
			if v == NotStarted{
				reply.TaskName = k
				reply.ReduceNum = c.NumReduceWorker
				// 记录该文件正在工作，启动超时等待
				c.Mapfiles[k] = Processing

				go c.HandleTimeOut(Map,k)
				return nil
			}
		}
		//没有需要进行的map任务，返回sleep
		reply.TaskType = Sleep
		return nil

	}else if c.State == Reduce{
		reply.TaskType = Reduce
		for k,_ := range c.Reducefiles{
			if c.Reducefiles[k] == NotStarted{
				// reduce 的逻辑，获取需要reduce的文件名
				tempname,_ := strconv.Atoi(k)
				files := strings.Split(c.ReduceRecord[tempname], " ") 
				c.Reducefiles[k] = Processing
				reply.InputName = files
				go c.HandleTimeOut(Reduce, k)
				return nil
			}
		}
		reply.TaskType = Sleep
		return nil

		// 记录正在reduce的文件，启动超时等待
	}else{
		log.Fatal("wrong state")
	}



	return nil
}

func (c *Coordinator) HandleTimeOut(TaskType int,TaskName string) error{

	time.Sleep(time.Second*10)

	c.Mux.Lock()
	defer c.Mux.Unlock()
	if TaskType == Map{
		
		if c.Mapfiles[TaskName] != Finished{
			c.Mapfiles[TaskName] = NotStarted

		}
	}else if TaskType == Reduce{
		if c.Reducefiles[TaskName] != Finished{
			c.Reducefiles[TaskName] = NotStarted

		}

	}
	return nil
}






func (c *Coordinator) ResponseTask(args *FinishTaskRequest, reply *FinishTaskReply) error {
	c.Mux.Lock()
	defer c.Mux.Unlock()
	if args.TaskType == Map{
		if c.Mapfiles[args.TaskName] == Processing{
		c.Mapfiles[args.TaskName] = Finished
		// 存储中间件文件名
		for _,v := range args.FileName{
			tempstring := strings.Split(v, "-")
			index := tempstring[len(tempstring)-1]
			temp, err := strconv.Atoi(index)
			if err != nil {
				return nil
			}
			if c.ReduceRecord[temp] != ""{
				c.ReduceRecord[temp] = c.ReduceRecord[temp]+ " " + v
			}else{
				c.ReduceRecord[temp] = v
			}

		}
		//检查map过程是否已经结束
		flag := true
		for _,v:=range c.Mapfiles{
			if v == NotStarted || v == Processing{
				flag = false
			}
		}
		if flag == true{
			c.State = Reduce
		}		
	}else{
		return nil
	}

	}else if args.TaskType == Reduce{
		index := args.FileName[0][strings.LastIndex(args.FileName[0],"-")+1:]			
		if c.Reducefiles[index] == Processing{

			c.Reducefiles[index] = Finished

			//检查reduce过程是否已经结束
			flag := true
			for _,v:=range c.Reducefiles{
				if v == NotStarted || v == Processing{
					flag = false
				}
			}
			if flag == true{
				c.State = Finish
			}
		}else{
			return nil
		}

			
	}

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
	c.Mux.Lock()
	defer c.Mux.Unlock()
	if c.State == Finish{
		return true
	}
	return false

	// Your code here.
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//


func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fmt.Println("start")
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.

	
    tempreducerecord := make(map[int] string,nReduce)

    tempmapfiles := make(map[string] int,len(files))
    tempreducefiles := make(map[string]int ,nReduce)

	for _,i := range files{
		tempmapfiles[i] = 0
	}
	for i := 0 ; i < nReduce ; i++{
		tempreducefiles[strconv.Itoa(i)] = 0
	}
	for i := 0 ; i < nReduce ; i++{
		tempreducerecord[i] = ""
	}


	// ** record map num and send by apply to worker
   
	c := Coordinator{NumReduceWorker:nReduce,NumMapFile:len(files),State:1,
	ReduceRecord:tempreducerecord,Mapfiles:tempmapfiles,
	Reducefiles:tempreducefiles,MapTaskNum:0}

	// Your code here.


	c.server()
	return &c
}
