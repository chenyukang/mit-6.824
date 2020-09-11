package mr

import "log"
import "net"
import "os"
import "fmt"
import "time"
import "net/rpc"
import "net/http"

type JobInfo struct {
	status       string
	started_time time.Time
}

type Master struct {
	// Your definitions here.
	nReduce int
	file_kv map[string]JobInfo
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) DispatchJob(args *MrArgs, reply *MrReply) error {
	fmt.Fprintf(os.Stderr, "Got worker: %v\n", args.WORKER_NAME)
	reply.FILE_NAME = "hello world"
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

	if len(m.file_kv) == 0 {
		fmt.Fprintf(os.Stderr, "Don't have jobs\n")
		return false
	}

	// Your code here.
	for _, job := range m.file_kv {
		if job.status != "finished" {
			return ret
		}

	}
	return true
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.file_kv = make(map[string]JobInfo)
	for _, filename := range files {
		fmt.Fprintf(os.Stderr, "open file: %v\n", filename)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		file.Close()
		m.file_kv[filename] = JobInfo{"pending", time.Now()}
	}
	m.server()
	return &m
}
