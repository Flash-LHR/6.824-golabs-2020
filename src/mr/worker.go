package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

const (
	intermediateFileNameFormat string = "mr-%d-%d.json" // mr-<mapID>-<reduceID>.json
	temporaryFileNameFormat    string = "mr-temp-%d"    // mr-temp-<reduceID>
	outputFileNameFormat       string = "mr-out-%d"     // mr-out-<reduceID>
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func getReduceId(key string, nReduce int) int {
	return ihash(key) % nReduce
}

func writeIntermediateFile(fileName string, keyValues []KeyValue) error {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Printf("Failed to open file \"%v\" in map task\n", fileName)
		return err
	}
	defer file.Close()
	enc := json.NewEncoder(file)
	for _, kv := range keyValues {
		err := enc.Encode(&kv)
		if err != nil {
			log.Printf("Failed to encode intermediate keyValue %v", kv)
			return err
		}
	}
	return nil
}

func readIntermediateFile(fileName string) ([]KeyValue, error) {
	keyValues := make([]KeyValue, 0)
	file, err := os.Open(fileName)
	if err != nil {
		log.Printf("Failed to open file \"%v\" in reduce task\n", fileName)
		return nil, err
	}
	defer file.Close()
	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		keyValues = append(keyValues, kv)
	}
	return keyValues, nil
}

func writeTemporaryFile(fileName string, keyValues []KeyValue) error {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Printf("Failed to open file \"%v\" in reduce task\n", fileName)
		return err
	}
	defer file.Close()
	for _, keyValue := range keyValues {
		fmt.Fprintf(file, "%v %v\n", keyValue.Key, keyValue.Value)
	}
	return nil
}

func processMapTask(mapTask MapTaskReply, mapf func(string, string) []KeyValue) {
	// read file content
	content, err := ioutil.ReadFile(mapTask.InputFileName)
	if err != nil {
		log.Printf("Failed to read file \"%v\"", mapTask.InputFileName)
		return
	}
	// get KeyValue emitted by mapf
	mapKeyValue := mapf(mapTask.InputFileName, string(content))
	// shuffle
	intermediateKeyValue := make([][]KeyValue, mapTask.NReduce)
	for _, keyValue := range mapKeyValue {
		fileId := getReduceId(keyValue.Key, mapTask.NReduce)
		if intermediateKeyValue[fileId] == nil {
			intermediateKeyValue[fileId] = make([]KeyValue, 0)
		}
		intermediateKeyValue[fileId] = append(intermediateKeyValue[fileId], keyValue)
	}
	// write intermediate KeyValue to intermediate file
	finishMapTaskArgs := FinishMapTaskArgs{RunID: mapTask.RunID, IntermediateFileNames: make([]string, 0)}
	finishMapTaskReply := FinishMapTaskReply{}
	for i := 0; i < mapTask.NReduce; i++ {
		sort.Sort(ByKey(intermediateKeyValue[i]))
		fileName := fmt.Sprintf(intermediateFileNameFormat, mapTask.RunID, i)
		if err := writeIntermediateFile(fileName, intermediateKeyValue[i]); err != nil {
			log.Printf("Failed to write intermediate file \"%s\"\n", fileName)
			return
		}
		finishMapTaskArgs.IntermediateFileNames = append(finishMapTaskArgs.IntermediateFileNames, fileName)
	}
	// rpc call Master.FinishMapTask to notice master
	call("Master.FinishMapTask", &finishMapTaskArgs, &finishMapTaskReply)

	log.Printf("Finish map task with runID %v and input file \"%s\"\n", mapTask.RunID, mapTask.InputFileName)
}

func processReduceTask(reduceTask ReduceTaskReply, reducef func(string, []string) string) {
	intermediateKeyValues := make([]KeyValue, 0)
	for _, fileName := range reduceTask.InputFileNames {
		if keyValues, err := readIntermediateFile(fileName); err != nil {
			log.Printf("Failed to read from file \"%s\"\n", fileName)
			return
		} else {
			intermediateKeyValues = append(intermediateKeyValues, keyValues...)
		}
	}
	sort.Sort(ByKey(intermediateKeyValues))
	temporaryKeyValues := make([]KeyValue, 0)
	for l, r := 0, -1; l < len(intermediateKeyValues); l = r + 1 {
		for r+1 < len(intermediateKeyValues) && intermediateKeyValues[l].Key == intermediateKeyValues[r+1].Key {
			r++
		}
		mergedValues := make([]string, r-l+1)
		for i := 0; i < r-l+1; i++ {
			mergedValues[i] = intermediateKeyValues[l+i].Value
		}
		temporaryKeyValues = append(temporaryKeyValues, KeyValue{
			Key:   intermediateKeyValues[l].Key,
			Value: reducef(intermediateKeyValues[l].Key, mergedValues),
		})
	}
	temporaryFileName := fmt.Sprintf(temporaryFileNameFormat, reduceTask.RunID)
	outputFileName := fmt.Sprintf(outputFileNameFormat, reduceTask.TaskID)
	writeTemporaryFile(temporaryFileName, temporaryKeyValues)
	os.Rename(temporaryFileName, outputFileName)

	finishReduceTaskArgs := FinishReduceTaskArgs{
		RunID:          reduceTask.RunID,
		outputFileName: outputFileName,
	}
	finishReduceTaskReply := FinishReduceTaskReply{}

	call("Master.FinishReduceTask", &finishReduceTaskArgs, &finishReduceTaskReply)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		args := AssignTaskArgs{}
		reply := AssignTaskReply{}
		if !call("Master.AssignTask", &args, &reply) {
			return
		}
		log.Printf("AssignTask response from Master with reply: %#v\n", reply)
		switch reply.TaskType {
		case mapTaskTypeEnum:
			processMapTask(reply.MapTask, mapf)
		case reduceTaskTypeEnum:
			processReduceTask(reply.ReduceTask, reducef)
		case waitTaskTypeEnum:
			time.Sleep(time.Second)
		case exitTaskTypeEnum:
			return
		default:
			log.Fatalf("Unexpected task type %v\n", reply.TaskType)
		}
	}
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
