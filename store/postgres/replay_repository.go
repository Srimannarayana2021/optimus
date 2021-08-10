package postgres

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/odpf/optimus/core/tree"

	"gorm.io/datatypes"

	"github.com/google/uuid"
	"github.com/jinzhu/gorm"
	"github.com/odpf/optimus/models"
	"github.com/odpf/optimus/store"
)

type Replay struct {
	ID uuid.UUID `gorm:"primary_key;type:uuid"`

	JobID uuid.UUID `gorm:"not null"`
	Job   Job       `gorm:"foreignKey:JobID"`

	StartDate     time.Time `gorm:"not null"`
	EndDate       time.Time `gorm:"not null"`
	Status        string    `gorm:"not null"`
	Message       datatypes.JSON
	ExecutionTree datatypes.JSON

	CreatedAt time.Time `gorm:"not null" json:"created_at"`
	UpdatedAt time.Time `gorm:"not null" json:"updated_at"`
}

type ExecutionTree struct {
	Data       interface{}
	Dependents []*ExecutionTree
	Runs       []time.Time
}

func fromTreeNode(executionTree *tree.TreeNode) *ExecutionTree {
	var dependents []*ExecutionTree
	for _, dependent := range executionTree.Dependents {
		dependents = append(dependents, fromTreeNode(dependent))
	}

	var runs []time.Time
	for _, run := range executionTree.Runs.Values() {
		runs = append(runs, run.(time.Time))
	}

	return &ExecutionTree{
		Data:       executionTree.Data.(models.JobSpec),
		Dependents: dependents,
		Runs:       runs,
	}
}

func (p Replay) FromSpec(spec *models.ReplaySpec) (Replay, error) {
	message, err := json.Marshal(spec.Message)
	if err != nil {
		return Replay{}, nil
	}

	var executionTree []byte
	if spec.ExecutionTree != nil {
		executionTree, err = json.Marshal(fromTreeNode(spec.ExecutionTree))
		if err != nil {
			return Replay{}, err
		}
	}

	return Replay{
		ID:            spec.ID,
		JobID:         spec.Job.ID,
		StartDate:     spec.StartDate.UTC(),
		EndDate:       spec.EndDate.UTC(),
		Status:        spec.Status,
		Message:       message,
		ExecutionTree: executionTree,
	}, nil
}

func toTreeNode(executionTree *ExecutionTree) *tree.TreeNode {
	var jobSpec models.JobSpec
	if err := mapstructure.Decode(executionTree.Data, &jobSpec); err != nil {
		return nil
	}
	treeNode := tree.NewTreeNode(jobSpec)
	for _, dependent := range executionTree.Dependents {
		treeNode.AddDependent(toTreeNode(dependent))
	}
	for _, run := range executionTree.Runs {
		treeNode.Runs.Add(run)
	}
	return treeNode
}

func (p Replay) ToSpec(jobSpec models.JobSpec) (models.ReplaySpec, error) {
	message := models.ReplayMessage{}
	if err := json.Unmarshal(p.Message, &message); err != nil {
		return models.ReplaySpec{}, nil
	}
	executionTree := ExecutionTree{}
	if p.ExecutionTree != nil {
		if err := json.Unmarshal(p.ExecutionTree, &executionTree); err != nil {
			return models.ReplaySpec{}, err
		}
	}
	return models.ReplaySpec{
		ID:            p.ID,
		Job:           jobSpec,
		Status:        p.Status,
		StartDate:     p.StartDate,
		EndDate:       p.EndDate,
		Message:       message,
		ExecutionTree: toTreeNode(&executionTree),
		CreatedAt:     p.CreatedAt,
	}, nil
}

type replayRepository struct {
	DB      *gorm.DB
	adapter *JobSpecAdapter
	hash    models.ApplicationKey
}

func NewReplayRepository(db *gorm.DB, jobAdapter *JobSpecAdapter, hash models.ApplicationKey) *replayRepository {
	return &replayRepository{
		DB:      db,
		adapter: jobAdapter,
		hash:    hash,
	}
}

func (repo *replayRepository) Insert(replay *models.ReplaySpec) error {
	r, err := Replay{}.FromSpec(replay)
	if err != nil {
		return err
	}
	return repo.DB.Create(&r).Error
}

func (repo *replayRepository) GetByID(id uuid.UUID) (models.ReplaySpec, error) {
	var r Replay
	if err := repo.DB.Where("id = ?", id).Preload("Job").Preload("Job.Project").
		Preload("Job.Project.Secrets").Find(&r).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return models.ReplaySpec{}, store.ErrResourceNotFound
		}
		return models.ReplaySpec{}, err
	}
	jobSpec, err := repo.adapter.ToSpec(r.Job)
	if err != nil {
		return models.ReplaySpec{}, err
	}
	projectSpec, err := r.Job.Project.ToSpecWithSecrets(repo.hash)
	if err != nil {
		return models.ReplaySpec{}, err
	}
	jobSpec.Project = projectSpec
	return r.ToSpec(jobSpec)
}

func (repo *replayRepository) UpdateStatus(replayID uuid.UUID, status string, message models.ReplayMessage) error {
	var r Replay
	if err := repo.DB.Where("id = ?", replayID).Find(&r).Error; err != nil {
		return errors.New("could not update non-existing replay")
	}
	jsonBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}
	r.Status = status
	r.Message = jsonBytes
	return repo.DB.Save(&r).Error
}

func (repo *replayRepository) GetByStatus(status []string) ([]models.ReplaySpec, error) {
	var replays []Replay
	if err := repo.DB.Where("status in (?)", status).Preload("Job").
		Preload("Job.Project").Preload("Job.Project.Secrets").Find(&replays).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return []models.ReplaySpec{}, store.ErrResourceNotFound
		}
		return []models.ReplaySpec{}, err
	}

	var replaySpecs []models.ReplaySpec
	for _, r := range replays {
		jobSpec, err := repo.adapter.ToSpec(r.Job)
		if err != nil {
			return []models.ReplaySpec{}, err
		}
		projectSpec, err := r.Job.Project.ToSpecWithSecrets(repo.hash)
		if err != nil {
			return []models.ReplaySpec{}, err
		}
		jobSpec.Project = projectSpec

		replaySpec, err := r.ToSpec(jobSpec)
		if err != nil {
			return []models.ReplaySpec{}, err
		}
		replaySpecs = append(replaySpecs, replaySpec)
	}
	return replaySpecs, nil
}

func (repo *replayRepository) GetByJobIDAndStatus(jobID uuid.UUID, status []string) ([]models.ReplaySpec, error) {
	var replays []Replay
	if err := repo.DB.Where("job_id = ? and status in (?)", jobID, status).Preload("Job").Find(&replays).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return []models.ReplaySpec{}, store.ErrResourceNotFound
		}
		return []models.ReplaySpec{}, err
	}

	var replaySpecs []models.ReplaySpec
	for _, r := range replays {
		jobSpec, err := repo.adapter.ToSpec(r.Job)
		if err != nil {
			return []models.ReplaySpec{}, err
		}
		replaySpec, err := r.ToSpec(jobSpec)
		if err != nil {
			return []models.ReplaySpec{}, err
		}
		replaySpecs = append(replaySpecs, replaySpec)
	}
	return replaySpecs, nil
}
