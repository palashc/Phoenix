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
	workerGod "phoenix/worker-god"
	"strconv"
	"time"
)

var frc = flag.String("conf", config.DefaultConfigPath, "config file")

func noError(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

const ZK_DETECT_TIME = 3 * time.Second

func main() {
	flag.Parse()

	rc, e := config.LoadConfig(*frc)
	noError(e)

	// create just one workerGodClient for single-node configuration
	workerGodClients := make([]phoenix.WorkerGod, 0)

	for _, addr := range rc.WorkerGods {
		workerGodClients = append(workerGodClients, workerGod.GetNewClient(addr))
	}

	// spawn new monitors and executors
	for i := range rc.Monitors {
		var ret bool
		if e := workerGodClients[i].Start(i, &ret); e != nil || !ret {
			panic(e)
		}
	}
	// sleep for enough time for zookeeper to find monitors
	time.Sleep(ZK_DETECT_TIME)

	fmt.Println("Instantiated Monitors and Executors")

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
	<-feConfig.Ready

	// TODO: randomize number of jobs or tasks
	numTasks := 10
	numJobs := 20

	jobList := make([]*types.Job, 20)
	var sumOfTaskTimes float64 = 0

	// populate jobList
	for i := 0; i < numJobs; i++ {
		jobid := "job" + strconv.Itoa(i)
		tasks := make([]types.Task, 0)
		for j := 0; j < numTasks; j++ {
			taskid := jobid + "-task" + strconv.Itoa(j)

			taskTime := rand.Float64()
			sumOfTaskTimes += taskTime
			task := types.Task{JobId: jobid, Id: taskid, T: taskTime}

			tasks = append(tasks, task)
		}

		jobList[i] = &types.Job{
			Id:    jobid,
			Tasks: tasks,

			// TODO: Currently we only test with one frontend
			OwnerAddr: feAddr,
		}
	}

	allJobsDoneSignal := make(chan bool)

	// time taken by jobs
	var timeTaken float64

	// run jobs
	startTime := time.Now()
	for i := 0; i < numJobs; i++ {
		// TODO: does using go routines here improve performance?
		sendJobsChan <- jobList[i]
	}

	// wait for all jobs to end
	go func() {
		for i := 0; i < numJobs; i++ {
			fmt.Println("finished job: ", <-jobDoneChan)
		}
		timeTaken = time.Since(startTime).Seconds()
		allJobsDoneSignal <- true
	}()

	time.Sleep(ZK_DETECT_TIME)

	// randomly kill and spawn monitors
	workersKilled := make([]int, 0)
	for i := 0; len(rc.Monitors)-len(workersKilled) > 1; i++ {
		var ret bool
		if e := workerGodClients[i].Kill(i, &ret); e != nil || !ret {
			panic(e)
		}
		fmt.Println("Killed workerId:", i)
		time.Sleep(1 * time.Second)

		workersKilled = append(workersKilled, i)
	}

	// sleep for Kills to take affect
	time.Sleep(ZK_DETECT_TIME)

	// bring workers back
	for i, id := range workersKilled {
		var ret bool
		if e := workerGodClients[i].Start(id, &ret); e != nil || !ret {
			panic(e)
		}
		fmt.Println("Brought back workerId:", id)
		time.Sleep(1 * time.Second)
	}

	// sleep for Kills to take affect
	time.Sleep(ZK_DETECT_TIME)

	fmt.Println("Done Sleeping")
	<-allJobsDoneSignal

	slotCount := len(rc.Executors) * rc.NumSlots
	theoreticalLowerBound := sumOfTaskTimes / float64(slotCount)

	overhead := 100 * (timeTaken/theoreticalLowerBound - 1)

	fmt.Printf("Complete time taken for test in seconds: %f\n", timeTaken)
	fmt.Printf("Total time of jobs in seconds: %f\n", sumOfTaskTimes)
	fmt.Printf("We are within %f percent of the theoretical lower bound\n", overhead)

	writeToFile(rc, numJobs, numTasks, timeTaken, overhead)
}

func writeToFile(rc *config.PhoenixConfig, jobCount, taskCount int, timeTaken, overhead float64) error {
	logName := "logs/" + strconv.Itoa(len(rc.Schedulers)) + "_sched:" + strconv.Itoa(len(rc.Monitors)) + "_mtor:" +
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
