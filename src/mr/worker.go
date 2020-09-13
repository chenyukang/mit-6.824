package mr

import "fmt"
import "log"
import "os"
import "math/rand"
import "time"
import "net/rpc"
import "hash/fnv"
import "sort"
import "io/ioutil"
import "strings"
import "encoding/json"
import "path/filepath"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
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

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyz")

func RandWorkerName(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func trim(s string) string {
	tokens := strings.Split(s, "/")
	return tokens[len(tokens)-1]
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	rand.Seed(time.Now().UnixNano())
	for true {
		more := TryGetJob(mapf, reducef)
		if !more {
			break
		} else {
			//time.Sleep(time.Second)
		}
	}

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func TryGetJob(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {

	// declare an argument structure.
	args := MrArgs{RandWorkerName(4), "", 0}

	// declare a reply structure.
	reply := MrReply{}

	// send the RPC request, wait for the reply.
	call("Master.DispatchJob", &args, &reply)

	if os.Getenv("MR_DEBUG") == "YES" {
		fmt.Printf("got file_name: %v\n", reply.FILE_NAME)
		fmt.Printf("got job_type: %v\n", reply.JOB_TYPE)
		fmt.Printf("got job_index: %v\n", reply.JOB_INDEX)
	}
	switch reply.JOB_TYPE {
	case "map":
		runMap(mapf, reply.FILE_NAME, reply.JOB_INDEX, reply.REDUCE_COUNT)
		return true
	case "reduce":
		runReduce(reducef, reply.FILE_NAME, reply.JOB_INDEX)
		return true
	case "wait":
		return true
	}
	// no more job
	return false
}

func runMap(mapf func(string, string) []KeyValue, filename string,
	job_index int, reduce_count int) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	//fmt.Printf("kva len: %v\n", len(kva))

	keyMap := make(map[int][]KeyValue)
	for _, kv := range kva {
		n := ihash(kv.Key) % reduce_count
		if _, ok := keyMap[n]; !ok {
			keyMap[n] = []KeyValue{}
		}
		keyMap[n] = append(keyMap[n], kv)
	}

	for k, v := range keyMap {
		file, err := os.Create(fmt.Sprintf("mr-%v-%v.map", job_index, k))
		if err != nil {
			fmt.Println(err)
		}
		enc := json.NewEncoder(file)
		enc.Encode(&v)
		file.Close()
	}

	args := MrArgs{filename, "map", job_index}
	reply := MrReply{}

	call("Master.FinishJob", &args, &reply)
}

func runReduce(reducef func(string, []string) string, filename string, job_index int) {
	pat := fmt.Sprintf("mr-*-%v.map", job_index)
	matches, _ := filepath.Glob(pat)
	intermediate := []KeyValue{}
	for _, match := range matches {
		file, err := os.Open(match)
		if err != nil {
			fmt.Println(err)
		}
		dec := json.NewDecoder(file)
		for {
			var kva []KeyValue
			if err := dec.Decode(&kva); err != nil {
				break
			}
			intermediate = append(intermediate, kva...)
		}

		file.Close()
	}

	sort.Sort(ByKey(intermediate))
	if os.Getenv("MR_DEBUG") == "YES" {
		fmt.Printf("reduce job %v has len %v of intermediates\n",
			job_index, len(intermediate))
	}
	i := 0
	var sb strings.Builder
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
		sb.WriteString(fmt.Sprintf("%v %v\n", intermediate[i].Key, output))
		i = j
	}

	ofile, _ := os.Create(fmt.Sprintf("mr-out-%v", job_index))
	fmt.Fprintf(ofile, sb.String())
	ofile.Close()

	args := MrArgs{filename, "reduce", job_index}
	reply := MrReply{}

	call("Master.FinishJob", &args, &reply)
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
