package memory

import (
	_ "google.golang.org/genproto/protobuf/ptype" //nolint:revive,nolintlint

	"github.com/roadrunner-server/api/v4/plugins/v1/kv"
	"github.com/roadrunner-server/api/v4/plugins/v4/jobs"
	"github.com/roadrunner-server/endure/v2/dep"
	"github.com/roadrunner-server/memory/v5/memoryjobs"
	"github.com/roadrunner-server/memory/v5/memorykv"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

const PluginName string = "memory"

type Plugin struct {
	log    *zap.Logger
	cfg    Configurer
	tracer *sdktrace.TracerProvider
}

type Logger interface {
	NamedLogger(name string) *zap.Logger
}

type Tracer interface {
	Tracer() *sdktrace.TracerProvider
}

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if a config section exists.
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

func (p *Plugin) Collects() []*dep.In {
	return []*dep.In{
		dep.Fits(func(pp any) {
			p.tracer = pp.(Tracer).Tracer()
		}, (*Tracer)(nil)),
	}
}

// Drivers implementation

func (p *Plugin) KvFromConfig(_ string) (kv.Storage, error) {
	return memorykv.NewInMemoryDriver(p.log, p.tracer), nil
}

// DriverFromConfig constructs a memory driver from the .rr.yaml configuration
func (p *Plugin) DriverFromConfig(configKey string, pq jobs.Queue, pipeline jobs.Pipeline) (jobs.Driver, error) {
	return memoryjobs.FromConfig(p.tracer, configKey, p.log, p.cfg, pipeline, pq)
}

// DriverFromPipeline constructs a memory driver from a pipeline
func (p *Plugin) DriverFromPipeline(pipe jobs.Pipeline, pq jobs.Queue) (jobs.Driver, error) {
	return memoryjobs.FromPipeline(p.tracer, pipe, p.log, pq)
}
