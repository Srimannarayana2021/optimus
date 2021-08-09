package job

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/odpf/optimus/core/logger"

	"github.com/odpf/optimus/models"
	"github.com/pkg/errors"
)

const (
	AirflowClearDagRunFailed = "failed to clear airflow dag run"
)

type ReplayWorker interface {
	Process(context.Context, uuid.UUID) error
}

type replayWorker struct {
	replaySpecRepoFac ReplaySpecRepoFactory
	scheduler         models.SchedulerUnit
}

func (w *replayWorker) Process(ctx context.Context, reqUUID uuid.UUID) (err error) {
	replaySpecRepo := w.replaySpecRepoFac.New()
	// mark replay request in progress
	if inProgressErr := replaySpecRepo.UpdateStatus(reqUUID, models.ReplayStatusInProgress, models.ReplayMessage{}); inProgressErr != nil {
		return inProgressErr
	}

	replaySpec, err := replaySpecRepo.GetByID(reqUUID)
	if err != nil {
		return err
	}

	replayDagsMap := replaySpec.ExecutionTree.GetAllNodes()
	for _, treeNode := range replayDagsMap {
		runTimes := treeNode.Runs.Values()
		startTime := runTimes[0].(time.Time)
		endTime := runTimes[treeNode.Runs.Size()-1].(time.Time)
		if err = w.scheduler.Clear(ctx, replaySpec.Job.Project, treeNode.GetName(), startTime, endTime); err != nil {
			err = errors.Wrapf(err, "error while clearing dag runs for job %s", treeNode.GetName())
			logger.W(fmt.Sprintf("error while running replay %s: %s", reqUUID.String(), err.Error()))
			if updateStatusErr := replaySpecRepo.UpdateStatus(reqUUID, models.ReplayStatusFailed, models.ReplayMessage{
				Type:    AirflowClearDagRunFailed,
				Message: err.Error(),
			}); updateStatusErr != nil {
				return updateStatusErr
			}
			return err
		}
	}

	if err = replaySpecRepo.UpdateStatus(reqUUID, models.ReplayStatusReplayed, models.ReplayMessage{}); err != nil {
		return err
	}
	logger.I(fmt.Sprintf("successfully cleared instances of replay id: %s", reqUUID.String()))
	return nil
}

func NewReplayWorker(replaySpecRepoFac ReplaySpecRepoFactory, scheduler models.SchedulerUnit) *replayWorker {
	return &replayWorker{replaySpecRepoFac: replaySpecRepoFac, scheduler: scheduler}
}
