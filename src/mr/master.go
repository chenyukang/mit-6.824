package mr

import "log"
import "net"
import "os"
import "fmt"
import "time"
import "sync"
import "net/rpc"
import "net/http"

type JobInfo struct {
	status    string
	startTime time.Time
}

type Master struct {
	// Your definitions here.
	mu          sync.Mutex
	nReduce     int
	fileStats   map[string]string
	mapJobs     map[string]JobInfo
	reduceJobs  map[int]JobInfo
	reduceCount int
	mapIndex    int
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) DispatchJob(args *MrArgs, reply *MrReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if os.Getenv("MR_DEBUG") == "YES" {
		fmt.Fprintf(os.Stderr, "Got worker: %v\n", args.NAME)
	}
	mappedCount := 0
	for file, status := range m.fileStats {
		if status == "pending" {
			reply.FILE_NAME = file
			reply.JOB_TYPE = "map"
			reply.JOB_INDEX = m.mapIndex
			reply.REDUCE_COUNT = m.nReduce
			m.fileStats[file] = "mapping"
			m.mapIndex++
			m.mapJobs[file] = JobInfo{"running", time.Now()}
			return nil
		} else if status == "mapped" {
			mappedCount++
		}
	}
	if mappedCount < len(m.fileStats) {
		reply.JOB_TYPE = "wait"
		return nil
	} else if mappedCount == len(m.fileStats) {
		for i := 0; i < m.nReduce; i++ {
			if _, ok := m.reduceJobs[i]; !ok {
				m.reduceJobs[i] = JobInfo{"running", time.Now()}
				reply.JOB_TYPE = "reduce"
				reply.JOB_INDEX = i
				reply.REDUCE_COUNT = m.nReduce
				return nil
			}
		}
	}
	reply.JOB_TYPE = "no_job"
	return nil
}

func (m *Master) FinishJob(args *MrArgs, reply *MrReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	//fmt.Fprintf(os.Stderr, "Finished Worker: %v\n", args.NAME)
	if args.JOB_TYPE == "map" {
		filename := args.NAME
		//job_index := args.JOB_INDEX
		m.fileStats[filename] = "mapped"
		delete(m.mapJobs, filename)
	} else {
		m.reduceJobs[args.JOB_INDEX] = JobInfo{"finished", time.Now()}
	}
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

func (m *Master) CheckJobs(timeout int) {
	for true {
		m.mu.Lock()
		cleanUp := []string{}
		for file, jobInfo := range m.mapJobs {
			now := time.Now()
			if jobInfo.status == "running" &&
				now.Sub(jobInfo.startTime).Seconds() >= float64(timeout) {
				fmt.Printf("dead job: %v\n", file)
				m.fileStats[file] = "pending"
				cleanUp = append(cleanUp, file)
			}
		}

		for _, file := range cleanUp {
			delete(m.mapJobs, file)
		}

		cleanUpIndex := []int{}
		for jobIndex, jobInfo := range m.reduceJobs {
			now := time.Now()
			if jobInfo.status == "running" &&
				now.Sub(jobInfo.startTime).Seconds() >= float64(timeout) {
				fmt.Printf("dead reduce: %v\n", jobIndex)
				cleanUpIndex = append(cleanUpIndex, jobIndex)
			}
		}
		for _, idx := range cleanUpIndex {
			delete(m.reduceJobs, idx)
		}
		m.mu.Unlock()
		time.Sleep(time.Second)
	}

}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if os.Getenv("MR_DEBUG") == "YES" {
		fmt.Fprintf(os.Stderr,
			"\n===============\nFile count: %v\nMap count: %v\nReduce count: %v\n",
			len(m.fileStats),
			len(m.mapJobs),
			len(m.reduceJobs))
	}

	if len(m.fileStats) == 0 {
		fmt.Fprintf(os.Stderr, "Don't have jobs\n")
		return false
	}

	if len(m.reduceJobs) < m.nReduce {
		return false
	}

	for _, jobInfo := range m.reduceJobs {
		if jobInfo.status != "finished" {
			return false
		}
	}

	fmt.Printf("ALL DONE!\n")
	return true
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.fileStats = make(map[string]string)
	for _, filename := range files {
		//fmt.Fprintf(os.Stderr, "open file: %v\n", filename)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		file.Close()
		m.fileStats[filename] = "pending"
	}
	m.mapJobs = make(map[string]JobInfo)
	m.reduceJobs = make(map[int]JobInfo)
	m.nReduce = nReduce
	m.mapIndex = 0
	go m.CheckJobs(10)
	m.server()
	return &m
}
