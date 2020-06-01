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

	fmt.Println("Parsed main")

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
	go frontend.ServeFrontend(feConfig)
	<- feConfig.Ready

	// TODO: randomize number of jobs or tasks
	numTasks := 10
	numJobs := 2

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

			// TODO: Currently we only test with one frontend
			OwnerAddr: feAddr,
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

	timeTaken := float32(time.Since(startTime).Seconds())

	slotCount := len(rc.Executors) * rc.NumSlots
	theoreticalLowerBound := sumOfTaskTimes / float32(slotCount)

	overhead := 100 * (timeTaken/theoreticalLowerBound - 1)

	fmt.Printf("Complete time taken for test in seconds: %f\n", timeTaken)
	fmt.Printf("Total time of jobs in seconds: %f\n", sumOfTaskTimes)
	fmt.Printf("We are within %f percent of the theoretical lower bound\n", overhead)

	writeToFile(rc, numJobs, numTasks, timeTaken, overhead)
}

func writeToFile(rc *config.PhoenixConfig, jobCount, taskCount int, timeTaken, overhead float32) error {
	logName := strconv.Itoa(len(rc.Schedulers)) + "_sched:" + strconv.Itoa(len(rc.Monitors)) + "_mtor:" +
		strconv.Itoa(rc.NumSlots) + "_slots" + strconv.Itoa(taskCount) + "_tasks.log"

	fout, e := rc.Write(logName)

	if e != nil {
		return e
	}

	fout.WriteString(fmt.Sprintf("\njobCount: %d, taskCount: %d, timeTaken: %f, overhead percent: %f\n",
		jobCount, taskCount, timeTaken, overhead))
	fout.Close()
	return nil
}
