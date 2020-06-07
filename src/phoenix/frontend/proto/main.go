package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"phoenix"
	"phoenix/config"
	"phoenix/scheduler"
	"phoenix/types"
	"strconv"
)

var (
	frc          = flag.String("conf", config.DefaultConfigPath, "config file")
	useRand      = flag.Bool("useRand", false, "use random seed to generate job, default to hash based on address")
	jobCount     = flag.Int("jobCount", 10, "number of job to generate")
	taskCount    = flag.Int("taskCount", 10, "number of task in a job")
	meanDuration = flag.Float64("jobDuration", 100.0, "job duration in ms")
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

	schedulerClientMap := make(map[string]phoenix.TaskSchedulerInterface)
	var sAddr string
	for _, schedulerAddr := range rc.Schedulers {
		sAddr = schedulerAddr
		schedulerClientMap[schedulerAddr] = scheduler.GetNewTaskSchedulerClient(schedulerAddr)
	}

	numTasks := *taskCount
	numJobs := *jobCount
	done := make(chan bool)

	jobFn := func(jobN int, done chan bool) {
		jobid := "job" + strconv.Itoa(jobN)
		job := types.Job{Id: jobid}
		tasks := []types.Task{}

		currTaskDuration := *meanDuration

		if *useRand {
			currTaskDuration *= rand.ExpFloat64()
		}

		for j := 0; j < numTasks; j++ {
			taskid := jobid + "-task" + strconv.Itoa(j)
			task := types.Task{JobId: jobid, Id: taskid, T: currTaskDuration}
			tasks = append(tasks, task)
		}
		job.Tasks = tasks
		var ret bool
		err := schedulerClientMap[sAddr].SubmitJob(job, &ret)
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println("Submitted job ", jobid, ret)
		done <- ret
	}

	for i := 0; i < numJobs; i++ {
		go jobFn(i, done)
	}

	for i := 0; i < numJobs; i++ {
		<-done
	}
}
