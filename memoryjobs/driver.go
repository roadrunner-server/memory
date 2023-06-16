package memoryjobs

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/roadrunner-server/api/v4/plugins/v2/jobs"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v4/utils"
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

	// v2023.1 OTEL
	tracerName string = "jobs"
)

var _ jobs.Driver = (*Driver)(nil)

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if config section exists.
	Has(name string) bool
}

type Config struct {
	Priority int64 `mapstructure:"priority"`
	Prefetch int64 `mapstructure:"prefetch"`
}

type Driver struct {
	cfg *Config

	// delayed messages
	delayed *int64
	// currently processing
	msgInFlight *int64
	// prefetch
	msgInFlightLimit *int64
	cond             sync.Cond

	tracer     *sdktrace.TracerProvider
	log        *zap.Logger
	pipeline   atomic.Pointer[jobs.Pipeline]
	pq         jobs.Queue
	localQueue chan *Item

	prop propagation.TextMapPropagator

	// time.sleep goroutines max number
	goroutines uint64

	priority  int64
	listeners uint32
	stopCh    chan struct{}
}

// FromConfig initializes kafka pipeline from the configuration
func FromConfig(
	tracer *sdktrace.TracerProvider,
	configKey string,
	log *zap.Logger,
	cfg Configurer,
	pipeline jobs.Pipeline,
	pq jobs.Queue) (*Driver, error) {
	const op = errors.Op("new_in_memory_pipeline")

	if tracer == nil {
		tracer = sdktrace.NewTracerProvider()
	}

	jb := &Driver{
		tracer:      tracer,
		cond:        sync.Cond{L: &sync.Mutex{}},
		log:         log,
		pq:          pq,
		goroutines:  0,
		msgInFlight: utils.Int64(0),
		delayed:     utils.Int64(0),
		stopCh:      make(chan struct{}),
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

	jb.msgInFlightLimit = utils.Int64(jb.cfg.Prefetch)

	if jb.cfg.Priority == 0 {
		jb.cfg.Priority = 10
	}

	jb.priority = jb.cfg.Priority
	jb.pipeline.Store(&pipeline)

	// initialize a local queue
	jb.localQueue = make(chan *Item, 100_000)
	jb.prop = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}, jprop.Jaeger{})
	otel.SetTextMapPropagator(jb.prop)

	return jb, nil
}

// FromPipeline initializes pipeline on-the-fly
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
		tracer:           tracer,
		log:              log,
		pq:               pq,
		cond:             sync.Cond{L: &sync.Mutex{}},
		localQueue:       make(chan *Item, 100_000),
		goroutines:       0,
		msgInFlight:      utils.Int64(0),
		msgInFlightLimit: utils.Int64(pref),
		delayed:          utils.Int64(0),
		priority:         pipeline.Priority(),
		stopCh:           make(chan struct{}),
	}

	dr.pipeline.Store(&pipeline)
	dr.prop = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}, jprop.Jaeger{})
	otel.SetTextMapPropagator(dr.prop)

	return dr, nil
}

func (c *Driver) Push(ctx context.Context, jb jobs.Message) error {
	const op = errors.Op("in_memory_push")

	ctx, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "in_memory_push")
	defer span.End()

	// check if the pipeline registered
	pipe := *c.pipeline.Load()
	if pipe == nil {
		return errors.E(op, errors.Errorf("no such pipeline: %s", jb.PipelineID()))
	}

	err := c.handleItem(ctx, fromJob(jb))
	if err != nil {
		return errors.E(op, err)
	}

	return nil
}

func (c *Driver) State(ctx context.Context) (*jobs.State, error) {
	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "in_memory_state")
	defer span.End()

	pipe := *c.pipeline.Load()
	return &jobs.State{
		Pipeline: pipe.Name(),
		Priority: uint64(pipe.Priority()),
		Driver:   pipe.Driver(),
		Queue:    pipe.Name(),
		Active:   atomic.LoadInt64(c.msgInFlight),
		Delayed:  atomic.LoadInt64(c.delayed),
		Ready:    ready(atomic.LoadUint32(&c.listeners)),
	}, nil
}

func (c *Driver) Run(ctx context.Context, pipe jobs.Pipeline) error {
	const op = errors.Op("in_memory_jobs_run")
	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "in_memory_run")
	defer span.End()

	t := time.Now().UTC()

	l := atomic.LoadUint32(&c.listeners)
	// listener already active
	if l == 1 {
		c.log.Warn("listener already in the active state")
		return errors.E(op, errors.Str("listener already in the active state"))
	}

	c.consume()
	atomic.StoreUint32(&c.listeners, 1)

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

	l := atomic.LoadUint32(&c.listeners)
	// no active listeners
	if l == 0 {
		return errors.Str("no active listeners, nothing to pause")
	}

	atomic.AddUint32(&c.listeners, ^uint32(0))

	// stop the Driver
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

	l := atomic.LoadUint32(&c.listeners)
	// listener already active
	if l == 1 {
		return errors.Str("memory listener is already in the active state")
	}

	// resume the Driver on the same channel
	c.consume()

	atomic.StoreUint32(&c.listeners, 1)
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
		break
	}

	// just to be sure, that queue won't block
	c.cond.Signal()

	// close localQueue channel
	close(c.localQueue)

	// help GC
	c.localQueue = nil

	c.log.Debug("pipeline was stopped", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.String("start", time.Now().UTC().String()), zap.String("elapsed", time.Since(start).String()))
	return nil
}

func (c *Driver) handleItem(ctx context.Context, msg *Item) error {
	const op = errors.Op("in_memory_handle_request")

	c.prop.Inject(ctx, propagation.HeaderCarrier(msg.headers))

	// handle timeouts
	// theoretically, some bad user may send millions requests with a delay and produce a billion (for example)
	// goroutines here. We should limit goroutines here.
	if msg.Options.Delay > 0 {
		// if we have 1000 goroutines waiting on the delay - reject 1001
		if atomic.LoadUint64(&c.goroutines) >= goroutinesMax {
			return errors.E(op, errors.Str("max concurrency number reached"))
		}

		go func(jj *Item) {
			atomic.AddUint64(&c.goroutines, 1)
			atomic.AddInt64(c.delayed, 1)

			defer atomic.AddUint64(&c.goroutines, ^uint64(0))

			time.Sleep(jj.Options.DelayDuration())

			select {
			case c.localQueue <- jj:
			default:
				c.log.Warn("can't push job", zap.String("error", "local queue closed or full"))
			}
		}(msg)

		return nil
	}

	// insert to the local, limited pipeline
	select {
	case c.localQueue <- msg:
		return nil
	case <-ctx.Done():
		return errors.E(op, errors.Errorf("local pipeline is full, consider to increase prefetch number, current limit: %d, context error: %v", c.cfg.Prefetch, ctx.Err()))
	}
}

func (c *Driver) consume() {
	go func() {
		// redirect
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

				for atomic.LoadInt64(c.msgInFlight) >= atomic.LoadInt64(c.msgInFlightLimit) {
					c.log.Debug("prefetch limit was reached, waiting for the jobs to be processed", zap.Int64("current", atomic.LoadInt64(c.msgInFlight)), zap.Int64("limit", atomic.LoadInt64(c.msgInFlightLimit)))
					// wait for the jobs to be processed
					c.cond.Wait()
				}

				if item.Priority() == 0 {
					item.Options.Priority = c.priority
				}

				// set requeue channel
				item.Options.requeueFn = c.handleItem
				item.Options.msgInFlight = c.msgInFlight
				item.Options.delayed = c.delayed
				item.Options.cond = &c.cond

				// inject OTEL headers
				c.prop.Inject(ctx, propagation.HeaderCarrier(item.headers))
				c.pq.Insert(item)

				// increase number of the active jobs
				atomic.AddInt64(c.msgInFlight, 1)

				c.log.Debug("message pushed to the priority queue", zap.Int64("current", atomic.LoadInt64(c.msgInFlight)), zap.Int64("limit", atomic.LoadInt64(c.msgInFlightLimit)))

				c.cond.L.Unlock()
				span.End()
			case <-c.stopCh:
				return
			}
		}
	}()
}

func ready(r uint32) bool {
	return r > 0
}
