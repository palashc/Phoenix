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

const DefaultRandSeed int64 = -13131313

var (
	frc          = flag.String("conf", config.DefaultConfigPath, "config file")
	useRand      = flag.Bool("useRand", false, "use random seed to generate job, default to hash based on address")
	jobCount     = flag.Int("jobCount", 10, "number of job to generate")
	taskCount    = flag.Int("taskCount", 10, "number of task in a job")
	meanDuration = flag.Float64("jobDuration", 3.0, "job duration in second")
	randSeed     = flag.Int64("randSeed", DefaultRandSeed, "task generation seed")
	gEmulation   = flag.Bool("gEmu", false, "use google cluster workload pattern")
)

func noError(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func main() {
	flag.Parse()

	rc, e := config.LoadConfig(*frc)
	noError(e)

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

	if *useRand {
		// The case where we did not pass in a seed but still want to be rand
		if *randSeed == DefaultRandSeed {
			rand.Seed(time.Now().UnixNano())
		} else {
			rand.Seed(*randSeed)
		}
	} else {
		// just some random number here for the purpose of predictable workload emulation
		var localRandSeed int64 = 1111
		rand.Seed(localRandSeed)
	}

	var gGenerator GoogleClusterTaskGenerator
	if *gEmulation {
		gGenerator = NewGoogleClusterTaskGenerator(*meanDuration, *randSeed, *taskCount)
	}

	numTasks := *taskCount
	numJobs := *jobCount

	jobList := make([]*types.Job, 20)
	var sumOfTaskTimes float64 = 0

	// populate jobList
	for i := 0; i < numJobs; i++ {
		jobid := "job" + strconv.Itoa(i)
		tasks := make([]types.Task, 0)

		currTaskDuration := *meanDuration

		if *useRand {
			currTaskDuration *= rand.ExpFloat64()
		}

		if *gEmulation {
			currTaskDuration = gGenerator.GetTaskDuration()
		}

		for j := 0; j < numTasks; j++ {
			taskid := jobid + "-task" + strconv.Itoa(j)

			sumOfTaskTimes += currTaskDuration
			task := types.Task{JobId: jobid, Id: taskid, T: currTaskDuration}

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

		// set time taken
		timeTaken = float64(time.Since(startTime).Seconds())
		allJobsDoneSignal <- true
	}()

	// We can use worker-god to start or kill more jobs here

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
