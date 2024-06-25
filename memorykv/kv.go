package memorykv

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/roadrunner-server/api/v4/plugins/v1/kv"
	"github.com/roadrunner-server/errors"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

const (
	tracerName = "inmemory"
)

type callback func(sCh <-chan struct{})

type cb struct {
	updateCh chan int // new ttl
	stopCh   chan struct{}
}

type Driver struct {
	heap sync.Map // map[string]kv.Item
	// callbacks contains all callbacks channels for the keys
	callbacks       sync.Map // map[string]*cb
	broadcastStopCh atomic.Pointer[chan struct{}]

	mapSize int64
	tracer  *sdktrace.TracerProvider
	log     *zap.Logger
	cfg     *Config
}

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if config section exists.
	Has(name string) bool
}

func NewInMemoryDriver(key string, log *zap.Logger, cfgPlugin Configurer, tracer *sdktrace.TracerProvider) (*Driver, error) {
	const op = errors.Op("new_in_memory_driver")

	if tracer == nil {
		tracer = sdktrace.NewTracerProvider()
	}

	d := &Driver{
		callbacks: sync.Map{},
		heap:      sync.Map{},
		mapSize:   0,
		log:       log,
		tracer:    tracer,
	}

	ch := make(chan struct{})
	d.broadcastStopCh.Store(&ch)

	err := cfgPlugin.UnmarshalKey(key, &d.cfg)
	if err != nil {
		return nil, errors.E(op, err)
	}

	if d.cfg == nil {
		return nil, errors.E(op, errors.Errorf("config not found by provided key: %s", key))
	}

	d.cfg.InitDefaults()

	return d, nil
}

func (d *Driver) Has(keys ...string) (map[string]bool, error) {
	const op = errors.Op("in_memory_plugin_has")

	_, span := d.tracer.Tracer(tracerName).Start(context.Background(), "inmemory:has")
	defer span.End()

	if keys == nil {
		span.RecordError(errors.Str("no keys provided"))
		return nil, errors.E(op, errors.NoKeys)
	}

	m := make(map[string]bool)
	for i := range keys {
		keyTrimmed := strings.TrimSpace(keys[i])
		if keyTrimmed == "" {
			return nil, errors.E(op, errors.EmptyKey)
		}

		if _, ok := d.heap.Load(keys[i]); ok {
			m[keys[i]] = true
		}
	}

	span.SetAttributes(attribute.Int64("allocated memory:bytes", d.loadAllocatedSize()))

	return m, nil
}

func (d *Driver) Get(key string) ([]byte, error) {
	const op = errors.Op("in_memory_plugin_get")

	_, span := d.tracer.Tracer(tracerName).Start(context.Background(), "inmemory:get")
	defer span.End()

	// to get cases like "  "
	keyTrimmed := strings.TrimSpace(key)
	if keyTrimmed == "" {
		span.RecordError(errors.Str("empty key"))
		return nil, errors.E(op, errors.EmptyKey)
	}

	if data, exist := d.heap.Load(key); exist {
		// here might be a panic
		// but data only could be a string, see Set function
		span.SetAttributes(attribute.Int64("allocated memory:bytes", d.loadAllocatedSize()))
		return data.(kv.Item).Value(), nil
	}

	span.SetAttributes(attribute.Int64("allocated memory:bytes", d.loadAllocatedSize()))

	return nil, nil
}

func (d *Driver) MGet(keys ...string) (map[string][]byte, error) {
	const op = errors.Op("in_memory_plugin_mget")
	_, span := d.tracer.Tracer(tracerName).Start(context.Background(), "inmemory:mget")
	defer span.End()

	if keys == nil {
		span.RecordError(errors.Str("no keys provided"))
		return nil, errors.E(op, errors.NoKeys)
	}

	// should not be empty keys
	for i := range keys {
		keyTrimmed := strings.TrimSpace(keys[i])
		if keyTrimmed == "" {
			span.RecordError(errors.Str("empty key"))
			return nil, errors.E(op, errors.EmptyKey)
		}
	}

	m := make(map[string][]byte, len(keys))

	for i := 0; i < len(keys); i++ {
		if value, ok := d.heap.Load(keys[i]); ok {
			m[keys[i]] = value.(kv.Item).Value()
		}
	}

	span.SetAttributes(attribute.Int64("allocated memory:bytes", d.loadAllocatedSize()))

	return m, nil
}

func (d *Driver) Set(items ...kv.Item) error {
	const op = errors.Op("in_memory_plugin_set")
	_, span := d.tracer.Tracer(tracerName).Start(context.Background(), "inmemory:set")
	defer span.End()

	if items == nil {
		span.RecordError(errors.Str("no items provided"))
		return errors.E(op, errors.NoKeys)
	}

	for i := range items {
		if items[i] == nil {
			continue
		}
		// TTL is set
		if items[i].Timeout() != "" {
			// check the TTL in the item
			tt, err := time.Parse(time.RFC3339, items[i].Timeout())
			if err != nil {
				span.RecordError(err)
				return err
			}

			tm := int(tt.UTC().Sub(time.Now().UTC()).Seconds())
			// we already in the future :)
			if tm < 0 {
				d.updateAllocatedSize(int64(len(items[i].Key()) + len(items[i].Value()) + len(items[i].Timeout())))
				// set item
				d.heap.Store(items[i].Key(), items[i])
				continue
			}

			// create callback to delete the key from the heap
			clbk, stopCh, updateCh := d.ttlcallback(items[i].Key(), tm)
			go func() {
				clbk(*d.broadcastStopCh.Load())
			}()

			// store the callback since we have TTL
			d.callbacks.Store(items[i].Key(), &cb{
				updateCh: updateCh,
				stopCh:   stopCh,
			})
		}

		d.updateAllocatedSize(int64(len(items[i].Key()) + len(items[i].Value()) + len(items[i].Timeout())))
		// set item
		d.heap.Store(items[i].Key(), items[i])
	}

	span.SetAttributes(attribute.Int64("allocated memory:bytes", d.loadAllocatedSize()))

	return nil
}

// MExpire sets the expiration time to the key
// If key already has the expiration time, it will be overwritten
func (d *Driver) MExpire(items ...kv.Item) error {
	const op = errors.Op("in_memory_plugin_mexpire")
	_, span := d.tracer.Tracer(tracerName).Start(context.Background(), "inmemory:mexpire")
	defer span.End()

	for i := range items {
		if items[i] == nil {
			continue
		}

		if items[i].Timeout() == "" || strings.TrimSpace(items[i].Key()) == "" {
			span.RecordError(errors.Str("timeout for MExpire is empty or key is empty"))
			return errors.E(op, errors.Str("timeout for MExpire is empty or key is empty"))
		}

		// check if the time is correct
		tm, err := time.Parse(time.RFC3339, items[i].Timeout())
		if err != nil {
			span.RecordError(err)
			return errors.E(op, err)
		}

		ttm := int(tm.UTC().Sub(time.Now().UTC()).Seconds())
		if ttm < 0 {
			// we're in the future, delete the item
			ttm = 0
		}

		if clb, ok := d.callbacks.Load(items[i].Key()); ok {
			// send new ttl to the callback
			clb.(*cb).updateCh <- ttm
		} else {
			// we should set the callback
			// create callback to delete the key from the heap
			clbk, stopCh, updateCh := d.ttlcallback(items[i].Key(), ttm)
			go func() {
				clbk(*d.broadcastStopCh.Load())
			}()

			d.callbacks.Store(items[i].Key(), &cb{
				updateCh: updateCh,
				stopCh:   stopCh,
			})
		}
	}

	span.SetAttributes(attribute.Int64("allocated memory:bytes", d.loadAllocatedSize()))

	return nil
}

func (d *Driver) TTL(keys ...string) (map[string]string, error) {
	const op = errors.Op("in_memory_plugin_ttl")
	_, span := d.tracer.Tracer(tracerName).Start(context.Background(), "inmemory:ttl")
	defer span.End()

	if keys == nil {
		span.RecordError(errors.Str("no keys provided"))
		return nil, errors.E(op, errors.NoKeys)
	}

	// should not be empty keys
	for i := range keys {
		keyTrimmed := strings.TrimSpace(keys[i])
		if keyTrimmed == "" {
			span.RecordError(errors.Str("empty key"))
			return nil, errors.E(op, errors.EmptyKey)
		}
	}

	m := make(map[string]string, len(keys))

	for i := range keys {
		if item, ok := d.heap.Load(keys[i]); ok {
			m[keys[i]] = item.(kv.Item).Timeout()
		}
	}

	span.SetAttributes(attribute.Int64("allocated memory:bytes", d.loadAllocatedSize()))

	return m, nil
}

func (d *Driver) Delete(keys ...string) error {
	const op = errors.Op("in_memory_plugin_delete")
	_, span := d.tracer.Tracer(tracerName).Start(context.Background(), "inmemory:delete")

	defer span.End()
	if keys == nil {
		span.RecordError(errors.Str("no keys provided"))
		return errors.E(op, errors.NoKeys)
	}

	// should not be empty keys
	for i := range keys {
		keyTrimmed := strings.TrimSpace(keys[i])
		if keyTrimmed == "" {
			span.RecordError(errors.Str("empty key"))
			return errors.E(op, errors.EmptyKey)
		}
	}

	for i := range keys {
		d.heap.Delete(keys[i])
		clbk, ok := d.callbacks.LoadAndDelete(keys[i])
		if ok {
			// send signal to stop the timer and delete the item
			clbk.(*cb).stopCh <- struct{}{}
		}
	}

	span.SetAttributes(attribute.Int64("allocated memory:bytes", d.loadAllocatedSize()))

	return nil
}

func (d *Driver) Clear() error {
	_, span := d.tracer.Tracer(tracerName).Start(context.Background(), "inmemory:clear")
	defer span.End()
	// stop all callbacks
	close(*d.broadcastStopCh.Load())

	newCh := make(chan struct{})
	d.broadcastStopCh.Swap(&newCh)

	d.heap.Range(func(key any, _ any) bool {
		d.heap.Delete(key)
		return true
	})

	// zero the allocated size
	atomic.StoreInt64(&d.mapSize, 0)

	return nil
}

func (d *Driver) updateAllocatedSize(newsize int64) {
	if newsize > 0 {
		atomic.AddInt64(&d.mapSize, newsize)
		return
	}

	curr := atomic.LoadInt64(&d.mapSize)
	if curr >= newsize {
		atomic.AddInt64(&d.mapSize, newsize)
	} else {
		atomic.StoreInt64(&d.mapSize, 0)
	}
}

func (d *Driver) loadAllocatedSize() int64 {
	return atomic.LoadInt64(&d.mapSize)
}

func (d *Driver) Stop() {
	close(*d.broadcastStopCh.Load())
}

// ================================== PRIVATE ======================================

func (d *Driver) ttlcallback(id string, ttl int) (callback, chan struct{}, chan int) {
	stopCbCh := make(chan struct{}, 1)
	updateTTLCh := make(chan int, 1)

	// at this point, when adding lock, we should not have the callback
	return func(sCh <-chan struct{}) {
		// ttl
		cbttl := ttl
		ta := time.NewTicker(time.Second * time.Duration(cbttl))
	loop:
		select {
		case <-ta.C:
			d.log.Debug("ttl expired",
				zap.String("id", id),
				zap.Int("ttl seconds", cbttl),
			)
			ta.Stop()
			// broadcast stop channel
		case <-sCh:
			d.log.Debug("ttl removed, broadcast call",
				zap.String("id", id),
				zap.Int("ttl seconds", cbttl),
			)
			ta.Stop()
			// item stop channel
		case <-stopCbCh:
			d.log.Debug("ttl removed, callback call",
				zap.String("id", id),
				zap.Int("ttl seconds", cbttl),
			)
			ta.Stop()
		case newTTL := <-updateTTLCh:
			d.log.Debug("updating ttl",
				zap.String("id", id),
				zap.Int("prev_ttl", cbttl),
				zap.Int("new_ttl", newTTL))
			// update callback TTL (for logs)
			cbttl = newTTL
			ta.Reset(time.Second * time.Duration(newTTL))
			// in case of TTL we don't need to remove the item, only update TTL
			goto loop
		}

		val, ok := d.heap.LoadAndDelete(id)
		if ok {
			// subtract the size of the item
			d.updateAllocatedSize(-int64(len(id) + len(val.(kv.Item).Value()) + len(val.(kv.Item).Timeout())))
		}

		d.callbacks.Delete(id)
	}, stopCbCh, updateTTLCh
}
