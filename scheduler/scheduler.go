package scheduler

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/golang/glog"
)

// Scheduler interface defines the set of methods a scheduler should implement
// and export as a RPC service.
type Scheduler interface {
	OnTaskFetched(task *Task, success *bool) error
	AdjustQuota(req *ChangeQuotaRequest, success *bool) error
	ConsumeQuota(req *ChangeQuotaRequest, success *bool) error
}

type AdsTaskScheduler struct {
	tasks                  chan string               // The pending tasks to schedule.
	capacity               int                       // The maximum number of pending tasks.
	taskQueueUrl           string                    // The queue URL for the incoming tasks.
	s3Client               *s3.S3                    // The s3 client to receive messages body from s3 db
	scheduledQueueUrl      string                    // Scheduled queue URL.
	scheduledTaskBuffer    *Task
	*QuotaManager          // The quota manager
}

func NewAdsTaskScheduler(taskQueueUrl string,
	scheduledQueueUrl string,
	scheduledTasksBufferSize int) *AdsTaskScheduler {
	s := new(AdsTaskScheduler)
	s.capacity = 1000
	s.tasks = make(chan string, s.capacity)
	s.taskQueueUrl = taskQueueUrl
	s.sqsAdsTaskQueue = sqs.New(sqs.NewSession())
	s.s3Client = s3.New(sqs.NewSession())
	s.scheduledTaskBuffer = NewTaskBuffer(
		scheduledQueueUrl, scheduledTasksBufferSize)
	return s
}

func (s *AdsTaskScheduler) PrettyPrint() string {
	return fmt.Sprintf("<html>Pending Tasks: %d<br>%s</html>",
		s.quota.PendingSize(),
		s.QuotaManager.PrettyPrint(func(taskType string) string {
			sq := s.quota.GetQuota(taskType)
			return fmt.Sprintf(
				"<br>AvgPri:%d&nbsp&nbspCount:%d",
				sq.AveragePriority(),
				sq.Len())
		}))
}

func (s *AdsTaskScheduler) Start() {
	s.QuotaManager.Start()

	rate := time.Second / time.Duration(Config.SchedulerMaxNumOfTasksPerSecond)
	throttle := time.Tick(rate)
	go ReceiveSQSMessageTillEndOfWorld(s.sqsAdsTaskQueue, s.s3Client,
		s.taskQueueUrl, func(message *sqs.Message) {
			var task AdsTask
			var success bool
			// Try parse as a single task.
			err := json.Unmarshal([]byte(*message.Body), &task)
			if err == nil {
				glog.V(5).Info("Parsed one single ads task.")
				<-throttle
				s.OnTaskFetched(&task, &success)
				return
			}
			// Try parse as array of tasks.
			var tasks []AdsTask
			err1 := json.Unmarshal([]byte(*message.Body), &tasks)
			if err1 == nil {
				glog.V(5).Info("Parsed a batch of ads tasks.")
				for i, _ := range tasks {
					<-throttle
					s.OnTaskFetched(&tasks[i], &success)
				}
			} else {
				glog.Error("Ads task parse failure: ",
					err.Error(), " and: ", err1.Error())
			}
		}, true, 1800)

	go s.distributeTask()
}

func (s *AdsTaskScheduler) OnTaskFetched(task *AdsTask, success *bool) error {
	glog.V(10).Info("A task being fetched: ", task.ID)
	// If TaskType is not specified, compute one.
	if len(task.TaskType) <= 0 {
		task.TaskType = GetType(task.ID)
	}
	s.tasks <- task
	*success = true
	return nil
}

func (s *AdsTaskScheduler) distributeTask() {
	for true {
		task := <-s.tasks
		typeQuota := s.quota.GetTypeQuota(task.TaskType)
		if typeQuota == nil {
			// We cannot process this type of ads task.
			// Log task.ID and record a counter on taskType.
			counterDimensions := BuildCounterDimensionsFromTask(task)
			glog.Warning("No processor avaiable: ", task.TaskType)
			PutMetric("scheduler-no-processor", 1, counterDimensions)
			continue
		}
		if task.Source == SourceOndemand {
			glog.Info("On demand ads task going to be scheduled: ",
				"Label: ", task.Label, " ID: ", task.ID)
		}
		typeQuota.Push(
			task,
			func(f *AdsTask) {
				counterDimensions := BuildCounterDimensionsFromTask(f)
				PutMetric("scheduler-out-of-quota", 1, counterDimensions)
				if task.Priority >= ReschedulePriority {
					glog.Warning("High priority task is dropped: ", f.ID,
						" with label: ", f.Label)
				}
			})
	}
}
