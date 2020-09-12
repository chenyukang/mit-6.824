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
	nReduce              int
	file_status          map[string]bool
	mapJobs              map[string]JobInfo
	reduceJobs           map[int]JobInfo
	finished_files_count int
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
	fmt.Fprintf(os.Stderr, "Jobs count: %v\n", len(m.file_status))
	if len(m.file_status) == 0 {
		fmt.Fprintf(os.Stderr, "Don't have jobs\n")
		return false
	}

	// Your code here.
	if len(m.file_status) == m.finished_files_count {
		return true
	}
	return false
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.file_status = make(map[string]bool)
	for _, filename := range files {
		fmt.Fprintf(os.Stderr, "open file: %v\n", filename)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		file.Close()
		m.file_status[filename] = false
	}
	m.mapJobs = make(map[string]JobInfo)
	m.reduceJobs = make(map[int]JobInfo)
	m.finished_files_count = 0
	m.server()
	return &m
}
