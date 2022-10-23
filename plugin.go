package memory

import (
	"github.com/roadrunner-server/memory/v3/memoryjobs"
	"github.com/roadrunner-server/memory/v3/memorykv"
	"github.com/roadrunner-server/sdk/v3/plugins/jobs"
	"github.com/roadrunner-server/sdk/v3/plugins/jobs/pipeline"
	"github.com/roadrunner-server/sdk/v3/plugins/kv"
	priorityqueue "github.com/roadrunner-server/sdk/v3/priority_queue"
	"go.uber.org/zap"
)

const PluginName string = "memory"

type Plugin struct {
	log *zap.Logger
	cfg Configurer
}

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if config section exists.
	Has(name string) bool
}

func (p *Plugin) Init(log *zap.Logger, cfg Configurer) error {
	p.log = new(zap.Logger)
	*p.log = *log
	p.cfg = cfg
	return nil
}

func (p *Plugin) Name() string {
	return PluginName
}

// Drivers implementation

func (p *Plugin) KvFromConfig(key string) (kv.Storage, error) {
	return memorykv.NewInMemoryDriver(key, p.log, p.cfg)
}

// ConsumerFromConfig creates new ephemeral consumer from the configuration
func (p *Plugin) ConsumerFromConfig(configKey string, pq priorityqueue.Queue) (jobs.Consumer, error) {
	return memoryjobs.FromConfig(configKey, p.log, p.cfg, pq)
}

// ConsumerFromPipeline creates new ephemeral consumer from the provided pipeline
func (p *Plugin) ConsumerFromPipeline(pipeline *pipeline.Pipeline, pq priorityqueue.Queue) (jobs.Consumer, error) {
	return memoryjobs.FromPipeline(pipeline, p.log, pq)
}
