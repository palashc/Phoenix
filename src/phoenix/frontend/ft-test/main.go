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

const DefaultRandSeed int64 = -13131313

var (
	frc          = flag.String("conf", config.DefaultConfigPath, "config file")
	useRand      = flag.Bool("useRand", false, "use random seed to generate job, default to hash based on address")
    meanDuration = flag.Float64("tasktime", 1.0, "task duration in second")
	seed         = flag.Int64("seed", DefaultRandSeed, "task generation seed")
	gEmulation   = flag.Bool("gEmu", false, "use google cluster workload pattern")
	jobSendGap	 = flag.Float64("sendGap", 1.0, "gap between consecutive job sends in second")

	killableWorkers = flag.Int("n", 1, "maximum number of workers to kill")
    recoverFlag = flag.Bool("r", false, "Recover killed workers?")
    workloadFlag = flag.String("w", "small", "Size of workload - small, medium, big")
)

func noError(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

const ZK_DETECT_TIME = 3 * time.Second

var workloadMap = map[string][]int{
	"small":  []int{50, 10},
	"medium": []int{100, 25},
	"big":    []int{200, 50},
}

func main() {
	flag.Parse()

	rc, e := config.LoadConfig(*frc)
	noError(e)

	workload, ok := workloadMap[*workloadFlag]
	if !ok {
		panic("Incorrect workload option")
	}
	if *killableWorkers >= len(rc.Monitors) {
		panic("Can not kill all monitors")
	}

	// create just one workerGodClient for single-node configuration
	var workerGodClients []phoenix.WorkerGod
	for i := 0; i < len(rc.WorkerGods); i++ {
		workerGodClients[i] = workerGod.GetNewClient(rc.WorkerGods[i])
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

	if *useRand {
		// The case where we did not pass in a seed but still want to be rand
		if *seed == DefaultRandSeed {
			rand.Seed(time.Now().UnixNano())
		} else {
			rand.Seed(*seed)
		}
	} else {
		// just some random number here for the purpose of predictable workload emulation
		var localRandSeed int64 = 1111
		rand.Seed(localRandSeed)
	}

	// TODO: randomize number of jobs or tasks
	numTasks := workload[1]
	numJobs := workload[0]

	var gGenerator frontend.GoogleClusterTaskGenerator
	if *gEmulation {
		gGenerator = frontend.NewGoogleClusterTaskGenerator(*meanDuration, *seed, numTasks)
	}

	jobList := make([]*types.Job, numJobs)
	var sumOfTaskTimes float64 = 0

	var jobTimes []float64
	var maxJobTime float64

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

		curJobTime := float64(numTasks) * currTaskDuration
		if curJobTime > maxJobTime {
			maxJobTime = curJobTime
		}

		jobTimes = append(jobTimes, curJobTime)

		for j := 0; j < numTasks; j++ {
			taskId := jobid + "-task" + strconv.Itoa(j)

			sumOfTaskTimes += currTaskDuration
			task := types.Task{JobId: jobid, Id: taskId, T: currTaskDuration}
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
	var startTime time.Time

	// run jobs
	go func() {
		startTime = time.Now()
		for i := 0; i < numJobs; i++ {
			fmt.Println("Sending job: ", jobList[i])
			sendJobsChan <- jobList[i]

			time.Sleep(time.Duration(1000 * *jobSendGap) * time.Millisecond)
		}
	}()

	// wait for all jobs to end
	go func() {
		for i := 0; i < numJobs; i++ {
			fmt.Println("finished job: ", <-jobDoneChan)
		}
		timeTaken = time.Since(startTime).Seconds()
		allJobsDoneSignal <- true
	}()

	time.Sleep(ZK_DETECT_TIME)

	//randomly kill and spawn monitors
	workersKilled := make([]int, 0)
	for i := 0; i < *killableWorkers; i++ {
		var ret bool
		if e := workerGodClients[i].Kill(i, &ret); e != nil || !ret {
			panic(e)
		}
		fmt.Println("Killed workerId:", i)
		time.Sleep(100 * time.Millisecond)

		workersKilled = append(workersKilled, i)
	}

	// sleep for Kills to take affect
	time.Sleep(ZK_DETECT_TIME)

	if *recoverFlag {
		// bring workers back
		for _, id := range workersKilled {
			var ret bool
			if e := workerGodClients[id].Start(id, &ret); e != nil || !ret {
				panic(e)
			}
			fmt.Println("Brought back workerId:", id)
			time.Sleep(1 * time.Second)
		}

		// sleep for Kills to take affect
		time.Sleep(ZK_DETECT_TIME)
	}

	fmt.Println("Done Sleeping")
	<-allJobsDoneSignal

	slotCount := len(rc.Executors) * rc.NumSlots
	theoreticalLowerBound := calcTheoreticalLowerBound(jobList, *jobSendGap, slotCount)

	overhead := 100 * (timeTaken/theoreticalLowerBound - 1)

	fmt.Printf("\n-----Summary---------\n")
	fmt.Printf("Workload: %d Jobs %d Tasks\n", workload[0], workload[1])
	fmt.Printf("Killed %d workers\n", *killableWorkers)
	fmt.Printf("Recovery: %t\n", *recoverFlag)
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

func calcTheoreticalLowerBound(jobList []*types.Job, jobGap float64, slotCount int) float64 {

	return recursiveLowerBound(jobList, make([]types.Task,0), 0, jobGap, slotCount)

}
func recursiveLowerBound (jobList []*types.Job, carryIn []types.Task, jobIdx int, jobGap float64, slotCount int) float64 {

	allTasks := append(carryIn, jobList[jobIdx].Tasks ...)

	totalTime := 0.0
	for _, t := range allTasks {
		totalTime += t.T
	}

	initialTotalTime := totalTime

	lastInclusiveIdx := len(allTasks) - 1

	// total computational time available across all slots
	totalWorkTime := float64(slotCount) * jobGap

	for totalTime > totalWorkTime {
		// we don't do the last task

		totalTime -= allTasks[lastInclusiveIdx].T
		lastInclusiveIdx --
	}

	remainingTasks := allTasks[lastInclusiveIdx+1:]

	// recursive base case
	if jobIdx == len(jobList)-1 {
		return initialTotalTime / float64(phoenix.MIN(slotCount, len(allTasks)))
	} else {
		return jobGap + recursiveLowerBound(jobList, remainingTasks, jobIdx + 1, jobGap, slotCount)
	}
}
