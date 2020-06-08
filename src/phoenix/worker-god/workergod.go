package worker_god

import (
	"bufio"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"os"
	"os/exec"
	"phoenix"
	"phoenix/config"
	"strconv"
	"time"
)


type WorkerWrapper struct {

	// map where key is worker index and value is processId for that worker
	RunningMonitors 	map[int] *exec.Cmd
	RunningExecutors 	map[int] *exec.Cmd

	// phoenix configuration
	Config 				*config.PhoenixConfig
}

func NewWorkerGod(config *config.PhoenixConfig) phoenix.WorkerGod {
	return &WorkerWrapper{
		RunningMonitors: make(map[int]*exec.Cmd),
		RunningExecutors: make(map[int]*exec.Cmd),
		Config: config,
	}
}

func (ww *WorkerWrapper) Kill(workerId int, ret *bool) error {

	fmt.Println("[WorkerWrapper: Kill] workerId:", workerId)

	killError := false
	errorString := ""

	mCmd, mExists := ww.RunningMonitors[workerId]
	if mExists {
		if err := mCmd.Process.Kill(); err != nil {
			killError = true
			errorString += fmt.Sprintf("Failed to kill monitor process for worker:%d.\n", workerId)
		}
	}

	// if mCmd exists, we hold an invariant that the respective eCmd must as well
	eCmd, eExists := ww.RunningExecutors[workerId]
	if eExists {
		if err := eCmd.Process.Kill(); err != nil {
			killError = true
			errorString += fmt.Sprintf("Failed to kill executor process for worker:%d.\n", workerId)
		}
	}

	// delete from hashmap if key exists
	delete(ww.RunningMonitors, workerId)
	delete(ww.RunningExecutors, workerId)

	if killError {
		*ret = false
		return errors.New(errorString)
	}

	*ret = true
	return nil
}

func (ww *WorkerWrapper) Start(workerId int, ret* bool) error {

	fmt.Println("[WorkerWrapper: Start] workerId:", workerId)

	errorString := ""
	startError := false

	_, mExists := ww.RunningMonitors[workerId]
	if mExists {
		startError = true
		errorString += fmt.Sprintf("Monitor process for worker:%d is already running.\n", workerId)
	}

	_, eExists := ww.RunningExecutors[workerId]
	if eExists {
		startError = true
		errorString += fmt.Sprintf("Executor process for worker:%d is already running.\n", workerId)
	}

	if startError {
		*ret = false
		return errors.New(errorString)
	}

	// by this point, we have ascertained that the monitor and executor for workerId do not exist
	mtor := exec.Command("init-monitor", "-workerId", strconv.Itoa(workerId))
	etor := exec.Command("init-executor", "-workerId", strconv.Itoa(workerId))


	ww.RunningMonitors[workerId] = mtor
	ww.RunningExecutors[workerId] = etor

	// extract stdout pipes
	mStdout, _ := mtor.StdoutPipe()
	eStdout, _ := etor.StdoutPipe()

	// start processes
	if err := mtor.Start(); err != nil {
		*ret = false
		return fmt.Errorf("Error starting monitor process for workerId:%d\n", workerId)
	}

	if err := etor.Start(); err != nil {
		// kill mtor command
		mtor.Process.Kill()

		*ret = false
		return fmt.Errorf("Error starting executor process for workerId:%d\n", workerId)
	}

	// save output to logs
	go writeToLog(mStdout, "logs/monitor_"+strconv.Itoa(workerId)+"_" +
		strconv.FormatInt(time.Now().Unix(), 10)+".log")
	go writeToLog(eStdout, "logs/executor_"+strconv.Itoa(workerId)+"_" +
		strconv.FormatInt(time.Now().Unix(), 10)+".log")

	*ret = true
	return nil
}

func writeToLog(out io.ReadCloser, logFileName string) {
	scanner := bufio.NewScanner(out)
	scanner.Split(bufio.ScanLines)

	logFile, _ := os.OpenFile(logFileName, os.O_CREATE | os.O_RDWR, 0777)
	for scanner.Scan() {
		m := scanner.Text()
		logFile.WriteString(m + "\n")
	}
}


