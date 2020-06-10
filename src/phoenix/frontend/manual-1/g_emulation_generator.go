package main

import "math/rand"

type GoogleClusterTaskGenerator struct {
	TaskDuration float64
	RandSeed     int64
	TaskCount    int
}

var googleTimeFactors = []int{
	51182,
	61100,
	76970,
	96318,
	102699,
	106596,
	110659,
	111951,
	112349,
	114887,
	123163,
	129392,
	129573,
	129698,
	129844,
	129954}

const GOOGLE_TIME_MAX_RANGE = 129954

func NewGoogleClusterTaskGenerator(taskDuration float64, randSeed int64, taskCount int) GoogleClusterTaskGenerator {
	ret := GoogleClusterTaskGenerator{
		TaskDuration: taskDuration,
		RandSeed:     randSeed,
		TaskCount:    taskCount,
	}

	rand.Seed(randSeed)
	return ret
}

func (g GoogleClusterTaskGenerator) GetTaskDuration() float64 {
	targetRange := rand.Int() % GOOGLE_TIME_MAX_RANGE

	for i := 0; i < len(googleTimeFactors); i++ {
		if googleTimeFactors[i] >= targetRange {
			return g.TaskDuration * (float64(i) + 1.0)
		}
	}

	return g.TaskDuration
}
