package memorykv

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/roadrunner-server/api/v4/plugins/v1/kv"
	"github.com/roadrunner-server/errors"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

const (
	tracerName = "inmemory"
)

type cb struct {
	updateCh chan int // new ttl
	stopCh   chan struct{}
}

type Driver struct {
	heap            *hmap
	mu              *sync.Mutex
	broadcastStopCh atomic.Pointer[chan struct{}]

	tracer *sdktrace.TracerProvider
	log    *zap.Logger
	cfg    *Config
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
		mu:     &sync.Mutex{},
		heap:   newHMap(),
		log:    log,
		tracer: tracer,
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

	// todo(rustatian): move this map to a sync.Pool?
	m := make(map[string]bool)
	for i := range keys {
		keyTrimmed := strings.TrimSpace(keys[i])
		if keyTrimmed == "" {
			return nil, errors.E(op, errors.EmptyKey)
		}

		if _, ok := d.heap.Get(keys[i]); ok {
			m[keys[i]] = true
		}
	}

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

	if data, exist := d.heap.Get(key); exist {
		// here might be a panic
		// but data only could be a string, see Set function
		return data.Value(), nil
	}

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
		if value, ok := d.heap.Get(keys[i]); ok {
			m[keys[i]] = value.Value()
		}
	}

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

		// check for the duplicates
		d.heap.Delete(items[i].Key())

		// at this point the duplicate key is removed

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
			if tm <= 0 {
				d.log.Warn("incorrect TTL time, saving without it", zap.String("key", items[i].Key()))
				// set item
				d.heap.Set(items[i].Key(), &Item{
					key:   items[i].Key(),
					value: items[i].Value(),
				})
				continue
			}

			// at this point, we have valid TTL and should save the item with the callback

			// create callback to delete the key from the heap
			stopCh, updateCh := d.ttlcallback(items[i].Key(), tm, *d.broadcastStopCh.Load())

			d.heap.Set(items[i].Key(), &Item{
				key:     items[i].Key(),
				value:   items[i].Value(),
				timeout: items[i].Timeout(),
				callback: &cb{
					updateCh: updateCh,
					stopCh:   stopCh,
				},
			})
		} else {
			// set item without TTL
			d.log.Debug("saving item without TTL", zap.String("key", items[i].Key()))
			d.heap.Set(items[i].Key(), &Item{
				key:   items[i].Key(),
				value: items[i].Value(),
			})
		}
	}

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

		if clb, ok := d.heap.Get(items[i].Key()); ok {
			// send new ttl to the callback
			clb.callback.updateCh <- ttm
		} else {
			// we should set the callback
			// create callback to delete the key from the heap
			stopCh, updateCh := d.ttlcallback(items[i].Key(), ttm, *d.broadcastStopCh.Load())
			d.heap.Set(items[i].Key(), &Item{
				key:     items[i].Key(),
				value:   items[i].Value(),
				timeout: items[i].Timeout(),
				callback: &cb{
					updateCh: updateCh,
					stopCh:   stopCh,
				},
			})
		}
	}

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
		if item, ok := d.heap.Get(keys[i]); ok {
			m[keys[i]] = item.Timeout()
		}
	}

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
		k, ok := d.heap.LoadAndDelete(keys[i])
		if ok {
			if k.callback != nil {
				// send signal to stop the timer and delete the item
				k.callback.stopCh <- struct{}{}
			}
		}
	}

	return nil
}

func (d *Driver) Clear() error {
	_, span := d.tracer.Tracer(tracerName).Start(context.Background(), "inmemory:clear")
	defer span.End()
	// stop all callbacks
	close(*d.broadcastStopCh.Load())

	newCh := make(chan struct{})
	d.broadcastStopCh.Swap(&newCh)
	d.heap.Clean()
	d.heap = newHMap()

	return nil
}

func (d *Driver) Stop() {
	close(*d.broadcastStopCh.Load())
}

// ================================== PRIVATE ======================================

func (d *Driver) ttlcallback(id string, ttl int, sCh <-chan struct{}) (chan struct{}, chan int) {
	stopCbCh := make(chan struct{}, 1)
	updateTTLCh := make(chan int, 1)

	go func(hid string) {
		// ttl
		cbttl := ttl
		ta := time.NewTicker(time.Second * time.Duration(cbttl))
		for {
			select {
			case <-ta.C:
				d.log.Debug("ttl expired",
					zap.String("id", hid),
					zap.Int("ttl seconds", cbttl),
				)
				ta.Stop()
				// removeEntry removes the entry w/o checking callback
				d.heap.removeEntry(hid)
				return
			case <-sCh:
				// broadcast stop channel
				d.log.Debug("ttl removed, broadcast call",
					zap.String("id", hid),
					zap.Int("ttl seconds", cbttl),
				)
				ta.Stop()
				// removeEntry removes the entry w/o checking callback
				d.heap.removeEntry(hid)
				return
			case <-stopCbCh:
				// item stop channel
				d.log.Debug("ttl removed, callback call",
					zap.String("id", hid),
					zap.Int("ttl seconds", cbttl),
				)
				return
			case newTTL := <-updateTTLCh:
				// in case of TTL we don't need to remove the item, only update TTL
				d.log.Debug("updating ttl",
					zap.String("id", hid),
					zap.Int("prev_ttl", cbttl),
					zap.Int("new_ttl", newTTL))
				// update callback TTL (for logs)
				cbttl = newTTL
				ta.Reset(time.Second * time.Duration(newTTL))
			}
		}
	}(id)
	return stopCbCh, updateTTLCh
}
