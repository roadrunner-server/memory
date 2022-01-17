package memory

import (
	"github.com/roadrunner-server/api/v2/plugins/cache"
	"github.com/roadrunner-server/api/v2/plugins/config"
	"github.com/roadrunner-server/api/v2/plugins/jobs"
	"github.com/roadrunner-server/api/v2/plugins/jobs/pipeline"
	"github.com/roadrunner-server/api/v2/plugins/kv"
	"github.com/roadrunner-server/api/v2/plugins/pubsub"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/memory/v2/memoryhttpcache"
	"github.com/roadrunner-server/memory/v2/memoryjobs"
	"github.com/roadrunner-server/memory/v2/memorykv"
	"github.com/roadrunner-server/memory/v2/memorypubsub"
	priorityqueue "github.com/roadrunner-server/sdk/v2/priority_queue"
	"go.uber.org/zap"
)

const PluginName string = "memory"

type Plugin struct {
	log *zap.Logger
	cfg config.Configurer
}

func (p *Plugin) Init(log *zap.Logger, cfg config.Configurer) error {
	p.log = new(zap.Logger)
	*p.log = *log
	p.cfg = cfg
	return nil
}

func (p *Plugin) Name() string {
	return PluginName
}

// Drivers implementation

func (p *Plugin) FromConfig(log *zap.Logger) (cache.Cache, error) {
	return memoryhttpcache.NewCacheDriver(log)
}

func (p *Plugin) PubSubFromConfig(key string) (pubsub.PubSub, error) {
	return memorypubsub.NewPubSubDriver(p.log, key)
}

func (p *Plugin) KvFromConfig(key string) (kv.Storage, error) {
	const op = errors.Op("memory_plugin_construct")
	st, err := memorykv.NewInMemoryDriver(key, p.log, p.cfg)
	if err != nil {
		return nil, errors.E(op, err)
	}
	return st, nil
}

// ConsumerFromConfig creates new ephemeral consumer from the configuration
func (p *Plugin) ConsumerFromConfig(configKey string, pq priorityqueue.Queue) (jobs.Consumer, error) {
	return memoryjobs.FromConfig(configKey, p.log, p.cfg, pq)
}

// ConsumerFromPipeline creates new ephemeral consumer from the provided pipeline
func (p *Plugin) ConsumerFromPipeline(pipeline *pipeline.Pipeline, pq priorityqueue.Queue) (jobs.Consumer, error) {
	return memoryjobs.FromPipeline(pipeline, p.log, pq)
}
