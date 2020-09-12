package mr

import "fmt"
import "log"
import "math/rand"
import "time"
import "net/rpc"
import "hash/fnv"

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

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz")

func RandWorkerName(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	rand.Seed(time.Now().UnixNano())
	TryGetJob()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func TryGetJob() {

	// declare an argument structure.
	args := MrArgs{RandWorkerName(4)}

	// declare a reply structure.
	reply := MrReply{}

	// send the RPC request, wait for the reply.
	call("Master.DispatchJob", &args, &reply)

	fmt.Printf("got file_name: %v\n", reply.FILE_NAME)
	fmt.Printf("got job_type: %v\n", reply.JOB_TYPE)
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
