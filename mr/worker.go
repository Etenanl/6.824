package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "time"
import "os"
import "io/ioutil"
import "encoding/json"
import "sort"
import "strings"
import "strconv"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		args := GetTaskRequest{}
		args.Index = -1
		reply := GetTaskReply{}
		call("Coordinator.GetTask",&args,&reply)
		if reply.TaskType == Sleep{
			time.Sleep(time.Millisecond*10)
		}else if reply.TaskType == Finish{
			return
		}else if reply.TaskType == Map{
			// map function
			filenames := make([]string,reply.ReduceNum)
			filenames = HandleMap(mapf,reply.TaskName,reply.ReduceNum,reply.Index)
			// report to coordinator
			report := FinishTaskRequest{}
			reportReply := FinishTaskReply{}
			report.FileName = filenames
			report.TaskType = reply.TaskType
			report.TaskName = reply.TaskName
			call("Coordinator.ResponseTask",&report,&reportReply)

		}else if reply.TaskType == Reduce{
			// reduce function
			oname := HandleReduce(reducef,reply.InputName)

			// report to coordinator
			report := FinishTaskRequest{}
			reportReply := FinishTaskReply{}
			report.FileName = append(report.FileName,oname)
			report.TaskType = reply.TaskType
			call("Coordinator.ResponseTask",&report,&reportReply)
		}else{
			log.Fatal("error : unknow TaskType")
		}
	}
}

func HandleMap(mapf func(string,string)[]KeyValue,filename string,reducenum int,mapnum int)[]string{

	//read input file and do map process
	intermediate := []KeyValue{}

	file, err := os.Open(filename)
	if err != nil{
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kav := mapf(filename,string(content))
	intermediate = append(intermediate,kav...)

	//create each json file if not exist
	filenames := make([]string,reducenum)
	basicname := "mr-" + strconv.Itoa(mapnum) + "-"
	files := make([]*os.File,reducenum)
	for i := 0 ; i < reducenum ; i++{
		filenames[i] = basicname + strconv.Itoa(i)
		files[i],_ = os.Create(filenames[i])
	}

	//write map result,**using lock here

	for _, kv := range intermediate{
		index := ihash(kv.Key)%reducenum
		enc := json.NewEncoder(files[index])

		enc.Encode(&kv)
	}
	return filenames


}

func HandleReduce(reducef func(string, []string) string,filenames []string)string{
	//read input file and sort all of them
	files:= make([]*os.File,len(filenames))

	intermediate := []KeyValue{}
	for i:=0;i<len(files);i++{
		file,err := os.Open(filenames[i])
		if err != nil {
			log.Fatalf("cannot open %v", filenames[i])
		}
		files[i] = file
		kv := KeyValue{}
		dec := json.NewDecoder(files[i])
		for{
			err:=dec.Decode(&kv)
			if err!=nil{
				break
			}
			intermediate = append(intermediate,kv)
		}
	}
	sort.Sort(ByKey(intermediate))

	//create output file
	oname := "mr-out-"
	
	index:=filenames[0][strings.LastIndex(filenames[0],"-")+1:]
	oname=oname+index
	if oname == "mr-out-0"{

	}
	ofile, _ := os.Create(oname)
	//write map result,**using lock here

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	return oname


}





//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.


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

	fmt.Println(err)
	return false
}
