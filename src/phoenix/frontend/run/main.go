package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"phoenix"
	"phoenix/config"
	"phoenix/frontend"
	"phoenix/scheduler"
	"phoenix/types"
	"strconv"
	"time"
)

var frc = flag.String("conf", config.DefaultConfigPath, "config file")

func noError(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func main() {
	flag.Parse()

	rc, e := config.LoadConfig(*frc)
	noError(e)

	schedulerClients := make([]phoenix.TaskSchedulerInterface, 0)
	for _, schedulerAddr := range rc.Schedulers {
		newSched := scheduler.GetNewTaskSchedulerClient(schedulerAddr)
		schedulerClients = append(schedulerClients, newSched)
	}

	// Only one frontend
	feAddr := rc.Frontends[0]
	jobDoneChan := make(chan string)
	fe, sendJobsChan := frontend.NewFrontend(feAddr, schedulerClients, jobDoneChan)

	feConfig := rc.NewFrontendConfig(0, fe)

	// run Frontend server
	noError(frontend.ServeFrontend(feConfig))

	// TODO: randomize number of jobs or tasks
	numTasks := 50
	numJobs := 20

	jobList := make([]*types.Job, 20)
	var sumOfTaskTimes float32 = 0

	// populate jobList
	for i := 0; i < numJobs; i++ {
		jobid := "job" + strconv.Itoa(i)
		tasks := make([]types.Task, 0)
		for j := 0; j < numTasks; j++ {
			taskid := jobid + "-task" + strconv.Itoa(j)

			taskTime := rand.Float32()
			sumOfTaskTimes += taskTime
			task := types.Task{JobId: jobid, Id: taskid, T: taskTime}

			tasks = append(tasks, task)
		}

		jobList[i] = &types.Job{
			Id: jobid,
			Tasks: tasks,
		}
	}

	// run jobs
	startTime := time.Now()
	for i := 0; i < numJobs; i++ {
		// TODO: does using go routines here improve performance?
		sendJobsChan <- jobList[i]
	}

	// wait for all jobs to end
	for i := 0; i < numJobs; i++ {
		<-jobDoneChan
	}
	endTime := time.Now()

	fmt.Printf("Complete time taken for test in nanoseconds: %d\n", endTime.UnixNano() - startTime.UnixNano())
	fmt.Printf("Total time of jobs in seconds: %f\n", sumOfTaskTimes)
}
