package memoryjobs

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goccy/go-json"
	"github.com/roadrunner-server/api/v4/plugins/v1/jobs"
	"github.com/roadrunner-server/sdk/v4/utils"
)

type Item struct {
	// Job contains name of job broker (usually PHP class).
	Job string `json:"job"`

	// Ident is unique identifier of the job, should be provided from outside
	Ident string `json:"id"`

	// Payload is string data (usually JSON) passed to Job broker.
	Payload string `json:"payload"`

	// Headers with key-values pairs
	Headers map[string][]string `json:"headers"`

	// Options contains set of PipelineOptions specific to job execution. Can be empty.
	Options *Options `json:"options,omitempty"`
}

// Options carry information about how to handle given job.
type Options struct {
	// Priority is job priority, default - 10
	// pointer to distinguish 0 as a priority and nil as priority not set
	Priority int64 `json:"priority"`

	// Pipeline manually specified pipeline.
	Pipeline string `json:"pipeline,omitempty"`

	// Delay defines time duration to delay execution for. Defaults to none.
	Delay int64 `json:"delay,omitempty"`

	// private
	requeueFn   func(context.Context, *Item) error
	cond        *sync.Cond
	msgInFlight *int64
	delayed     *int64
}

// DelayDuration returns delay duration in a form of time.Duration.
func (o *Options) DelayDuration() time.Duration {
	return time.Second * time.Duration(o.Delay)
}

func (i *Item) ID() string {
	return i.Ident
}

func (i *Item) Priority() int64 {
	return i.Options.Priority
}

// Body packs job payload into binary payload.
func (i *Item) Body() []byte {
	return utils.AsBytes(i.Payload)
}

// Context packs job context (job, id) into binary payload.
func (i *Item) Context() ([]byte, error) {
	ctx, err := json.Marshal(
		struct {
			ID       string              `json:"id"`
			Job      string              `json:"job"`
			Driver   string              `json:"driver"`
			Headers  map[string][]string `json:"headers"`
			Pipeline string              `json:"pipeline"`
		}{
			ID:       i.Ident,
			Job:      i.Job,
			Driver:   pluginName,
			Headers:  i.Headers,
			Pipeline: i.Options.Pipeline,
		},
	)

	if err != nil {
		return nil, err
	}

	return ctx, nil
}

func (i *Item) Metadata() map[string][]string {
	return i.Headers
}

func (i *Item) Ack() error {
	i.atomicallyReduceCount()
	return nil
}

func (i *Item) Nack() error {
	i.atomicallyReduceCount()
	return nil
}

func (i *Item) Requeue(headers map[string][]string, delay int64) error {
	// overwrite the delay
	i.Options.Delay = delay
	i.Headers = headers

	i.atomicallyReduceCount()

	err := i.Options.requeueFn(context.Background(), i)
	if err != nil {
		return err
	}

	return nil
}

// Respond for the in-memory is no-op
func (i *Item) Respond([]byte, string) error {
	return nil
}

// atomicallyReduceCount reduces counter of active or delayed jobs
func (i *Item) atomicallyReduceCount() {
	// reduce number of the all active jobs
	atomic.AddInt64(i.Options.msgInFlight, ^int64(0))
	// pass 1 job
	i.Options.cond.Signal()
	// if job was delayed, reduce number of the delayed jobs
	if i.Options.Delay > 0 {
		atomic.AddInt64(i.Options.delayed, ^int64(0))
	}
}

func fromJob(job jobs.Job) *Item {
	return &Item{
		Job:     job.Name(),
		Ident:   job.ID(),
		Payload: job.Payload(),
		Headers: job.Headers(),
		Options: &Options{
			Priority: job.Priority(),
			Pipeline: job.Pipeline(),
			Delay:    job.Delay(),
		},
	}
}
