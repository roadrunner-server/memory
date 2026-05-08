package memoryjobs

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/errors"
	jprop "go.opentelemetry.io/contrib/propagators/jaeger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

const (
	pluginName    string = "memory"
	prefetch      string = "prefetch"
	goroutinesMax uint64 = 1000

	tracerName string = "jobs"
)

var _ jobs.Driver = (*Driver)(nil)

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if a config section exists.
	Has(name string) bool
}

type Config struct {
	Priority int64 `mapstructure:"priority"`
	Prefetch int64 `mapstructure:"prefetch"`
}

type Driver struct {
	cfg *Config

	delayed          atomic.Int64
	msgInFlight      atomic.Int64
	msgInFlightLimit atomic.Int64
	cond             sync.Cond

	tracer     *sdktrace.TracerProvider
	log        *zap.Logger
	pipeline   atomic.Pointer[jobs.Pipeline]
	pq         jobs.Queue
	localQueue chan *Item

	prop propagation.TextMapPropagator

	goroutines atomic.Uint64

	priority  int64
	listeners atomic.Bool
	stopped   atomic.Bool
	stopCh    chan struct{}
}

func FromConfig(
	tracer *sdktrace.TracerProvider,
	configKey string,
	log *zap.Logger,
	cfg Configurer,
	pipeline jobs.Pipeline,
	pq jobs.Queue,
) (*Driver, error) {
	const op = errors.Op("new_in_memory_pipeline")

	if tracer == nil {
		tracer = sdktrace.NewTracerProvider()
	}

	jb := &Driver{
		tracer: tracer,
		cond:   sync.Cond{L: &sync.Mutex{}},
		log:    log,
		pq:     pq,
		stopCh: make(chan struct{}),
	}

	err := cfg.UnmarshalKey(configKey, &jb.cfg)
	if err != nil {
		return nil, errors.E(op, err)
	}

	if jb.cfg == nil {
		return nil, errors.E(op, errors.Errorf("config not found by provided key: %s", configKey))
	}

	if jb.cfg.Prefetch == 0 {
		jb.cfg.Prefetch = 100_000
	}

	jb.msgInFlightLimit.Store(jb.cfg.Prefetch)

	if jb.cfg.Priority == 0 {
		jb.cfg.Priority = 10
	}

	jb.priority = jb.cfg.Priority
	jb.pipeline.Store(&pipeline)

	jb.localQueue = make(chan *Item, 100_000)
	jb.prop = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}, jprop.Jaeger{})
	otel.SetTextMapPropagator(jb.prop)

	return jb, nil
}

func FromPipeline(
	tracer *sdktrace.TracerProvider,
	pipeline jobs.Pipeline,
	log *zap.Logger,
	pq jobs.Queue,
) (*Driver, error) {
	pref, err := strconv.ParseInt(pipeline.String(prefetch, "100000"), 10, 64)
	if err != nil {
		return nil, err
	}

	if tracer == nil {
		tracer = sdktrace.NewTracerProvider()
	}

	dr := &Driver{
		tracer:     tracer,
		log:        log,
		pq:         pq,
		cond:       sync.Cond{L: &sync.Mutex{}},
		localQueue: make(chan *Item, 100_000),
		priority:   pipeline.Priority(),
		stopCh:     make(chan struct{}),
	}

	dr.msgInFlightLimit.Store(pref)
	dr.pipeline.Store(&pipeline)
	dr.prop = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}, jprop.Jaeger{})
	otel.SetTextMapPropagator(dr.prop)

	return dr, nil
}

func (c *Driver) Push(ctx context.Context, jb jobs.Message) error {
	const op = errors.Op("in_memory_push")

	ctx, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "in_memory_push")
	defer span.End()

	pipe := *c.pipeline.Load()
	if pipe == nil {
		return errors.E(op, errors.Errorf("no such pipeline: %s", jb.GroupID()))
	}

	return errors.E(op, c.handleItem(ctx, fromJob(jb)))
}

func (c *Driver) State(ctx context.Context) (*jobs.State, error) {
	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "in_memory_state")
	defer span.End()

	pipe := *c.pipeline.Load()
	return &jobs.State{
		Pipeline: pipe.Name(),
		Priority: uint64(pipe.Priority()), //nolint:gosec
		Driver:   pipe.Driver(),
		Queue:    pipe.Name(),
		Active:   c.msgInFlight.Load(),
		Delayed:  c.delayed.Load(),
		Ready:    c.listeners.Load(),
	}, nil
}

func (c *Driver) Run(ctx context.Context, pipe jobs.Pipeline) error {
	const op = errors.Op("in_memory_jobs_run")
	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "in_memory_run")
	defer span.End()

	t := time.Now().UTC()

	if c.listeners.Load() {
		c.log.Warn("listener already in the active state")
		return errors.E(op, errors.Str("listener already in the active state"))
	}

	c.consume()
	c.listeners.Store(true)

	c.log.Debug("pipeline was started", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.String("start", time.Now().UTC().String()), zap.String("elapsed", time.Since(t).String()))
	return nil
}

func (c *Driver) Pause(ctx context.Context, p string) error {
	start := time.Now().UTC()
	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "in_memory_pause")
	defer span.End()

	pipe := *c.pipeline.Load()
	if pipe.Name() != p {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	if !c.listeners.Load() {
		return errors.Str("no active listeners, nothing to pause")
	}

	c.listeners.Store(false)

	c.stopCh <- struct{}{}
	c.log.Debug("pipeline was paused", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.String("start", time.Now().UTC().String()), zap.String("elapsed", time.Since(start).String()))

	return nil
}

func (c *Driver) Resume(ctx context.Context, p string) error {
	start := time.Now().UTC()
	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "in_memory_resume")
	defer span.End()
	pipe := *c.pipeline.Load()
	if pipe.Name() != p {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	if c.listeners.Load() {
		return errors.Str("memory listener is already in the active state")
	}

	c.consume()

	c.listeners.Store(true)
	c.log.Debug("pipeline was resumed", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.String("start", time.Now().UTC().String()), zap.String("elapsed", time.Since(start).String()))

	return nil
}

func (c *Driver) Stop(ctx context.Context) error {
	start := time.Now().UTC()
	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "in_memory_stop")
	defer span.End()

	pipe := *c.pipeline.Load()
	_ = c.pq.Remove(pipe.Name())

	select {
	case c.stopCh <- struct{}{}:
	default:
	}

	c.cond.Signal()

	close(c.localQueue)
	c.localQueue = nil
	c.stopped.Store(true)

	c.log.Debug("pipeline was stopped", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.String("start", time.Now().UTC().String()), zap.String("elapsed", time.Since(start).String()))
	return nil
}

func (c *Driver) handleItem(ctx context.Context, msg *Item) error {
	const op = errors.Op("in_memory_handle_request")

	c.prop.Inject(ctx, propagation.HeaderCarrier(msg.headers))

	if msg.Options.Delay > 0 {
		if c.goroutines.Load() >= goroutinesMax {
			return errors.E(op, errors.Str("max concurrency number reached"))
		}

		go func(jj *Item) {
			c.goroutines.Add(1)
			c.delayed.Add(1)

			defer c.goroutines.Add(^uint64(0))

			time.Sleep(jj.Options.DelayDuration())

			select {
			case c.localQueue <- jj:
			default:
				c.log.Warn("can't push job", zap.String("error", "local queue closed or full"))
			}
		}(msg)

		return nil
	}

	select {
	case c.localQueue <- msg:
		return nil
	case <-ctx.Done():
		return errors.E(op, errors.Errorf("local pipeline is full, consider to increase prefetch number, current limit: %d, context error: %v", c.cfg.Prefetch, ctx.Err()))
	}
}

func (c *Driver) consume() {
	go func() {
		for {
			select {
			case item, ok := <-c.localQueue:
				if !ok {
					c.log.Debug("ephemeral local queue closed")
					return
				}

				ctx := otel.GetTextMapPropagator().Extract(context.Background(), propagation.HeaderCarrier(item.headers))
				ctx, span := c.tracer.Tracer(tracerName).Start(ctx, "in_memory_listener")

				c.cond.L.Lock()

				for c.msgInFlight.Load() >= c.msgInFlightLimit.Load() {
					c.log.Debug("prefetch limit was reached, waiting for the jobs to be processed", zap.Int64("current", c.msgInFlight.Load()), zap.Int64("limit", c.msgInFlightLimit.Load()))
					c.cond.Wait()
				}

				if item.Priority() == 0 {
					item.Options.Priority = c.priority
				}

				item.Options.requeueFn = c.handleItem
				item.Options.msgInFlight = &c.msgInFlight
				item.Options.delayed = &c.delayed
				item.Options.cond = &c.cond
				item.Options.stopped = &c.stopped

				if item.headers == nil {
					item.headers = make(map[string][]string, 1)
				}

				c.prop.Inject(ctx, propagation.HeaderCarrier(item.headers))
				c.pq.Insert(item)

				c.msgInFlight.Add(1)

				c.log.Debug("message pushed to the priority queue", zap.Int64("current", c.msgInFlight.Load()), zap.Int64("limit", c.msgInFlightLimit.Load()))

				c.cond.L.Unlock()
				span.End()
			case <-c.stopCh:
				return
			}
		}
	}()
}
