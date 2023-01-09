package memoryjobs

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/roadrunner-server/api/v3/plugins/v1/jobs"
	pq "github.com/roadrunner-server/api/v3/plugins/v1/priority_queue"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/sdk/v4/utils"
	"go.uber.org/zap"
)

const (
	prefetch      string = "prefetch"
	goroutinesMax uint64 = 1000
)

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

type Consumer struct {
	cfg *Config

	// delayed messages
	delayed *int64
	// currently processing
	msgInFlight *int64
	// prefetch
	msgInFlightLimit *int64
	cond             sync.Cond

	log        *zap.Logger
	pipeline   atomic.Pointer[jobs.Pipeline]
	pq         pq.Queue
	localQueue chan *Item

	// time.sleep goroutines max number
	goroutines uint64

	priority  int64
	listeners uint32
	stopCh    chan struct{}
}

func FromConfig(configKey string, log *zap.Logger, cfg Configurer, pq pq.Queue) (*Consumer, error) {
	const op = errors.Op("new_ephemeral_pipeline")

	jb := &Consumer{
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

	// initialize a local queue
	jb.localQueue = make(chan *Item, 100_000)

	return jb, nil
}

func FromPipeline(pipeline jobs.Pipeline, log *zap.Logger, pq pq.Queue) (*Consumer, error) {
	pref, err := strconv.ParseInt(pipeline.String(prefetch, "100000"), 10, 64)
	if err != nil {
		return nil, err
	}

	return &Consumer{
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
	}, nil
}

func (c *Consumer) Push(ctx context.Context, jb jobs.Job) error {
	const op = errors.Op("ephemeral_push")
	// check if the pipeline registered
	pipe := *c.pipeline.Load()
	if pipe == nil {
		return errors.E(op, errors.Errorf("no such pipeline: %s", jb.Pipeline()))
	}

	err := c.handleItem(ctx, fromJob(jb))
	if err != nil {
		return errors.E(op, err)
	}

	return nil
}

func (c *Consumer) State(_ context.Context) (*jobs.State, error) {
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

func (c *Consumer) Register(_ context.Context, p jobs.Pipeline) error {
	c.pipeline.Store(&p)
	return nil
}

func (c *Consumer) Run(_ context.Context, pipe jobs.Pipeline) error {
	const op = errors.Op("memory_jobs_run")
	t := time.Now()

	l := atomic.LoadUint32(&c.listeners)
	// listener already active
	if l == 1 {
		c.log.Warn("listener already in the active state")
		return errors.E(op, errors.Str("listener already in the active state"))
	}

	c.consume()
	atomic.StoreUint32(&c.listeners, 1)

	c.log.Debug("pipeline was started", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.String("start", time.Now().String()), zap.String("elapsed", time.Since(t).String()))
	return nil
}

func (c *Consumer) Pause(_ context.Context, p string) {
	start := time.Now()
	pipe := *c.pipeline.Load()
	if pipe.Name() != p {
		c.log.Error("no such pipeline", zap.String("pause was requested: ", p))
	}

	l := atomic.LoadUint32(&c.listeners)
	// no active listeners
	if l == 0 {
		c.log.Warn("no active listeners, nothing to pause")
		return
	}

	atomic.AddUint32(&c.listeners, ^uint32(0))

	// stop the Consumer
	c.stopCh <- struct{}{}

	c.log.Debug("pipeline was paused", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.String("start", time.Now().String()), zap.String("elapsed", time.Since(start).String()))
}

func (c *Consumer) Resume(_ context.Context, p string) {
	start := time.Now()
	pipe := *c.pipeline.Load()
	if pipe.Name() != p {
		c.log.Error("no such pipeline", zap.String("resume was requested: ", p))
	}

	l := atomic.LoadUint32(&c.listeners)
	// listener already active
	if l == 1 {
		c.log.Warn("listener already in the active state")
		return
	}

	// resume the Consumer on the same channel
	c.consume()

	atomic.StoreUint32(&c.listeners, 1)
	c.log.Debug("pipeline was resumed", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.String("start", time.Now().String()), zap.String("elapsed", time.Since(start).String()))
}

func (c *Consumer) Stop(_ context.Context) error {
	start := time.Now()
	pipe := *c.pipeline.Load()

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

	c.log.Debug("pipeline was stopped", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.String("start", time.Now().String()), zap.String("elapsed", time.Since(start).String()))
	return nil
}

func (c *Consumer) handleItem(ctx context.Context, msg *Item) error {
	const op = errors.Op("ephemeral_handle_request")
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

func (c *Consumer) consume() {
	go func() {
		// redirect
		for {
			select {
			case item, ok := <-c.localQueue:
				if !ok {
					c.log.Debug("ephemeral local queue closed")
					return
				}

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

				c.pq.Insert(item)

				// increase number of the active jobs
				atomic.AddInt64(c.msgInFlight, 1)

				c.log.Debug("message pushed to the priority queue", zap.Int64("current", atomic.LoadInt64(c.msgInFlight)), zap.Int64("limit", atomic.LoadInt64(c.msgInFlightLimit)))

				c.cond.L.Unlock()
			case <-c.stopCh:
				return
			}
		}
	}()
}

func ready(r uint32) bool {
	return r > 0
}
