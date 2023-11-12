package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type TaskTypeEnum int
type TaskStatusEnum int

const (
	mapTaskTypeEnum TaskTypeEnum = iota
	reduceTaskTypeEnum
	waitTaskTypeEnum
	exitTaskTypeEnum
	idleTaskStatusEnum TaskStatusEnum = iota
	inProgressTaskStatusEnum
	completedTaskStatusEnum
)

const (
	backUpDurationSec int = 10
)

type mapTaskMasterInfoData struct {
	fileName      string
	status        TaskStatusEnum
	recentRunTime time.Time
}

type mapTaskMasterInProgressData struct {
	runID  int
	taskID int
}

type mapTaskMaster struct {
	runNum         int
	taskInfoData   []mapTaskMasterInfoData
	inProgressTask []mapTaskMasterInProgressData
}

type reduceTaskMasterInfoData struct {
	fileNames     []string
	status        TaskStatusEnum
	recentRunTime time.Time
}

type reduceTaskMasterInProgressData struct {
	runID  int
	taskID int
}

type reduceTaskMaster struct {
	runNum         int
	taskInfoData   []reduceTaskMasterInfoData
	inProgressTask []reduceTaskMasterInProgressData
}

type Master struct {
	nMap       int
	nReduce    int
	mapTask    mapTaskMaster
	reduceTask reduceTaskMaster
}

func (m *mapTaskMaster) hasTaskStatus(taskStatus TaskStatusEnum) (int, bool) {
	for i, task := range m.taskInfoData {
		if task.status == taskStatus {
			return i, true
		}
	}
	return -1, false
}

func (m *reduceTaskMaster) hasTaskStatus(taskStatus TaskStatusEnum) (int, bool) {
	for i, task := range m.taskInfoData {
		if task.status == taskStatus {
			return i, true
		}
	}
	return -1, false
}

func (m *Master) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	if idx, isExist := m.mapTask.hasTaskStatus(idleTaskStatusEnum); isExist {
		reply.TaskType = mapTaskTypeEnum
		reply.MapTask = MapTaskReply{
			NReduce:       m.nReduce,
			RunID:         m.mapTask.runNum,
			InputFileName: m.mapTask.taskInfoData[idx].fileName,
		}
		m.mapTask.taskInfoData[idx].recentRunTime = time.Now()
		m.mapTask.taskInfoData[idx].status = inProgressTaskStatusEnum
		m.mapTask.inProgressTask = append(m.mapTask.inProgressTask, mapTaskMasterInProgressData{
			runID:  m.mapTask.runNum,
			taskID: idx,
		})
		m.mapTask.runNum++
	} else if _, isExist := m.mapTask.hasTaskStatus(inProgressTaskStatusEnum); isExist {
		reply.TaskType = waitTaskTypeEnum
	} else if idx, isExist := m.reduceTask.hasTaskStatus(idleTaskStatusEnum); isExist {
		reply.TaskType = reduceTaskTypeEnum
		reply.ReduceTask = ReduceTaskReply{
			NMap:           m.nMap,
			RunID:          m.reduceTask.runNum,
			TaskID:         idx,
			InputFileNames: m.reduceTask.taskInfoData[idx].fileNames,
		}
		m.reduceTask.taskInfoData[idx].recentRunTime = time.Now()
		m.reduceTask.taskInfoData[idx].status = inProgressTaskStatusEnum
		m.reduceTask.inProgressTask = append(m.reduceTask.inProgressTask, reduceTaskMasterInProgressData{
			runID:  m.reduceTask.runNum,
			taskID: idx,
		})
		m.reduceTask.runNum++
	} else if _, isExist := m.reduceTask.hasTaskStatus(inProgressTaskStatusEnum); isExist {
		reply.TaskType = waitTaskTypeEnum
	} else {
		reply.TaskType = exitTaskTypeEnum
	}

	return nil
}

func (m *Master) FinishMapTask(args *FinishMapTaskArgs, reply *FinishMapTaskReply) error {
	if len(args.IntermediateFileNames) != m.nReduce {
		log.Printf("Incorrect IntermediateFileName slice size: %v\n", len(args.IntermediateFileNames))
		return errors.New("incorrect IntermediateFileName slice size")
	}
	// Find taskID with runID in inProgressTask
	var taskID int
	for _, task := range m.mapTask.inProgressTask {
		if task.runID == args.RunID {
			taskID = task.taskID
			break
		}
	}
	// Don't do anything when recive completedTask finishCall
	if m.mapTask.taskInfoData[taskID].status == completedTaskStatusEnum {
		return nil
	}
	// Mark task status with completed
	m.mapTask.taskInfoData[taskID].status = completedTaskStatusEnum
	log.Printf("Map task with taskID %v and file \"%s\" has finished\n", taskID, m.mapTask.taskInfoData[taskID].fileName)
	// Record intermediate files to reduce task
	for i := 0; i < m.nReduce; i++ {
		m.reduceTask.taskInfoData[i].fileNames = append(m.reduceTask.taskInfoData[i].fileNames,
			args.IntermediateFileNames[i])
	}
	// Remove all task with taskID from inProgressTask
	taskIdxs := make([]int, 0)
	for i := len(m.mapTask.inProgressTask) - 1; i >= 0; i-- {
		if m.mapTask.inProgressTask[i].taskID == taskID {
			taskIdxs = append(taskIdxs, i)
		}
	}
	if len(taskIdxs) == 0 {
		return nil
	}
	for _, taskIdx := range taskIdxs {
		m.mapTask.inProgressTask = append(m.mapTask.inProgressTask[:taskIdx],
			m.mapTask.inProgressTask[taskIdx+1:]...)
	}

	return nil
}

func (m *Master) FinishReduceTask(args *FinishReduceTaskArgs, reply *FinishReduceTaskReply) error {
	// Find taskID with runID in inProgressTask
	var taskID int
	for _, task := range m.reduceTask.inProgressTask {
		if task.runID == args.RunID {
			taskID = task.taskID
			break
		}
	}
	// Don't do anything when recive completedTask finishCall
	if m.reduceTask.taskInfoData[taskID].status == completedTaskStatusEnum {
		return nil
	}
	// Mark task status with completed
	m.reduceTask.taskInfoData[taskID].status = completedTaskStatusEnum
	log.Printf("Reduce task with taskID %v has finished\n", taskID)
	// Remove all task with taskID from inProgressTask
	taskIdxs := make([]int, 0)
	for i := len(m.reduceTask.inProgressTask) - 1; i >= 0; i-- {
		if m.reduceTask.inProgressTask[i].taskID == taskID {
			taskIdxs = append(taskIdxs, i)
		}
	}
	if len(taskIdxs) == 0 {
		return nil
	}
	for _, taskIdx := range taskIdxs {
		m.reduceTask.inProgressTask = append(m.reduceTask.inProgressTask[:taskIdx],
			m.reduceTask.inProgressTask[taskIdx+1:]...)
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	for i, task := range m.mapTask.taskInfoData {
		if task.status == inProgressTaskStatusEnum && int(time.Since(task.recentRunTime).Seconds()) > backUpDurationSec {
			m.mapTask.taskInfoData[i].status = idleTaskStatusEnum
		}
	}
	for i, task := range m.reduceTask.taskInfoData {
		if task.status == inProgressTaskStatusEnum && int(time.Since(task.recentRunTime).Seconds()) > backUpDurationSec {
			m.reduceTask.taskInfoData[i].status = idleTaskStatusEnum
		}
	}
	if _, isExist := m.mapTask.hasTaskStatus(idleTaskStatusEnum); isExist {
		return false
	}
	if _, isExist := m.mapTask.hasTaskStatus(inProgressTaskStatusEnum); isExist {
		return false
	}
	if _, isExist := m.reduceTask.hasTaskStatus(idleTaskStatusEnum); isExist {
		return false
	}
	if _, isExist := m.reduceTask.hasTaskStatus(inProgressTaskStatusEnum); isExist {
		return false
	}
	return true
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nMap:    len(files),
		nReduce: nReduce,
		mapTask: mapTaskMaster{
			runNum:         0,
			taskInfoData:   make([]mapTaskMasterInfoData, len(files)),
			inProgressTask: make([]mapTaskMasterInProgressData, 0),
		},
		reduceTask: reduceTaskMaster{
			runNum:         0,
			taskInfoData:   make([]reduceTaskMasterInfoData, nReduce),
			inProgressTask: make([]reduceTaskMasterInProgressData, 0),
		},
	}

	for i, fileName := range files {
		m.mapTask.taskInfoData[i] = mapTaskMasterInfoData{
			fileName: fileName,
			status:   idleTaskStatusEnum,
		}
	}

	for i := 0; i < m.nReduce; i++ {
		m.reduceTask.taskInfoData[i].status = idleTaskStatusEnum
	}

	m.server()
	return &m
}

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	logFile, err := os.OpenFile("/dev/null", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Panic("failed to open log file")
	}
	log.SetOutput(logFile)
}
