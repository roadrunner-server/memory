package memory

import (
	"github.com/roadrunner-server/api/v4/plugins/v1/jobs"
	"github.com/roadrunner-server/api/v4/plugins/v1/kv"
	pq "github.com/roadrunner-server/api/v4/plugins/v1/priority_queue"
	"github.com/roadrunner-server/memory/v4/memoryjobs"
	"github.com/roadrunner-server/memory/v4/memorykv"
	"go.uber.org/zap"
)

const PluginName string = "memory"

type Plugin struct {
	log *zap.Logger
	cfg Configurer
}

type Logger interface {
	NamedLogger(name string) *zap.Logger
}

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if config section exists.
	Has(name string) bool
}

func (p *Plugin) Init(log Logger, cfg Configurer) error {
	p.log = log.NamedLogger(PluginName)
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

// DriverFromConfig constructs memory driver from the .rr.yaml configuration
func (p *Plugin) DriverFromConfig(configKey string, pq pq.Queue, pipeline jobs.Pipeline, cmder chan<- jobs.Commander) (jobs.Driver, error) {
	return memoryjobs.FromConfig(configKey, p.log, p.cfg, pipeline, pq, cmder)
}

// DriverFromPipeline constructs memory driver from pipeline
func (p *Plugin) DriverFromPipeline(pipe jobs.Pipeline, pq pq.Queue, cmder chan<- jobs.Commander) (jobs.Driver, error) {
	return memoryjobs.FromPipeline(pipe, p.log, p.cfg, pq, cmder)
}
