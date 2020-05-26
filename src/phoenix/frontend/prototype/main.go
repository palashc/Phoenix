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

	schedulerClientMap := make(map[string]phoenix.TaskSchedulerInterface)
	schedulerArr := make([]phoenix.TaskSchedulerInterface,0)
	for _, schedulerAddr := range rc.Schedulers {
		newSched := scheduler.GetNewTaskSchedulerClient(schedulerAddr)
		schedulerArr = append(schedulerArr, newSched)
		schedulerClientMap[schedulerAddr] = newSched
	}

	numTasks := 1
	numJobs := 2
	done := make(chan bool)

	runJob := func(jobN int, done chan bool, sched phoenix.TaskSchedulerInterface) {
		jobid := "job" + strconv.Itoa(jobN)
		job := types.Job{Id: jobid}

		tasks := []types.Task{}
		for j := 0; j < numTasks; j++ {
			taskid := jobid + "-task" + strconv.Itoa(j)
			task := types.Task{JobId: jobid, Id: taskid, T: rand.Float32()}
			tasks = append(tasks, task)
		}
		job.Tasks = tasks
		var ret bool
		if err := sched.SubmitJob(job, &ret); err != nil {
			fmt.Println(err)
		}
		fmt.Println("Submitted job ", jobid, ret)
		done <- ret
	}

	for i := 0; i < numJobs; i++ {
		sched := schedulerArr[i % len(schedulerArr)]
		go runJob(i, done, sched)
	}

	for i := 0; i < numJobs; i++ {
		<-done
	}
}
