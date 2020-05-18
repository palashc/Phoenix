package phoenix

import "phoenix/types"

type MonitorInterface interface {
	EnqueueReservation(taskReservation *types.TaskReservation, position *int) error
	Probe(_ignore int, n *int) error
	TaskComplete(taskID string, ret *bool) error
	CancelTaskReservation(taskID string, ret *bool) error
}
