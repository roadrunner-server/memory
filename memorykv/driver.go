package memorykv

import (
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/roadrunner-server/api-plugins/v6/kv"
	"github.com/roadrunner-server/errors"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
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
	broadcastStopCh atomic.Pointer[chan struct{}]

	tracer *sdktrace.TracerProvider
	log    *slog.Logger
}

func NewInMemoryDriver(log *slog.Logger, tracer *sdktrace.TracerProvider) *Driver {
	if tracer == nil {
		tracer = sdktrace.NewTracerProvider()
	}

	d := &Driver{
		heap:   newHMap(),
		log:    log,
		tracer: tracer,
	}

	ch := make(chan struct{})
	d.broadcastStopCh.Store(&ch)

	return d
}

func (d *Driver) Has(ctx context.Context, keys ...string) (map[string]bool, error) {
	const op = errors.Op("in_memory_plugin_has")

	_, span := d.tracer.Tracer(tracerName).Start(ctx, "inmemory:has")
	defer span.End()

	if len(keys) == 0 {
		span.RecordError(errors.Str("no keys provided"))
		return nil, errors.E(op, errors.NoKeys)
	}

	m := make(map[string]bool, len(keys))
	for _, k := range keys {
		if strings.TrimSpace(k) == "" {
			return nil, errors.E(op, errors.EmptyKey)
		}

		if _, ok := d.heap.Get(k); ok {
			m[k] = true
		}
	}

	return m, nil
}

func (d *Driver) Get(ctx context.Context, key string) ([]byte, error) {
	const op = errors.Op("in_memory_plugin_get")

	_, span := d.tracer.Tracer(tracerName).Start(ctx, "inmemory:get")
	defer span.End()

	keyTrimmed := strings.TrimSpace(key)
	if keyTrimmed == "" {
		span.RecordError(errors.Str("empty key"))
		return nil, errors.E(op, errors.EmptyKey)
	}

	if data, exist := d.heap.Get(key); exist {
		return data.Value(), nil
	}

	return nil, nil
}

func (d *Driver) MGet(ctx context.Context, keys ...string) (map[string][]byte, error) {
	const op = errors.Op("in_memory_plugin_mget")
	_, span := d.tracer.Tracer(tracerName).Start(ctx, "inmemory:mget")
	defer span.End()

	if len(keys) == 0 {
		span.RecordError(errors.Str("no keys provided"))
		return nil, errors.E(op, errors.NoKeys)
	}

	m := make(map[string][]byte, len(keys))
	for _, k := range keys {
		if strings.TrimSpace(k) == "" {
			span.RecordError(errors.Str("empty key"))
			return nil, errors.E(op, errors.EmptyKey)
		}

		if value, ok := d.heap.Get(k); ok {
			m[k] = value.Value()
		}
	}

	return m, nil
}

func (d *Driver) Set(ctx context.Context, items ...kv.Item) error {
	const op = errors.Op("in_memory_plugin_set")
	_, span := d.tracer.Tracer(tracerName).Start(ctx, "inmemory:set")
	defer span.End()

	if len(items) == 0 {
		span.RecordError(errors.Str("no items provided"))
		return errors.E(op, errors.NoKeys)
	}

	for _, it := range items {
		if it == nil {
			continue
		}

		// check for the duplicates
		d.heap.Delete(it.Key())

		// TTL is set
		if it.Timeout() != "" {
			tt, err := time.Parse(time.RFC3339, it.Timeout())
			if err != nil {
				span.RecordError(err)
				return err
			}

			tm := int(tt.UTC().Sub(time.Now().UTC()).Seconds())
			if tm <= 0 {
				d.log.Warn("incorrect TTL time, saving without it", "key", it.Key())
				d.heap.Set(it.Key(), &Item{
					key:   it.Key(),
					value: it.Value(),
				})
				continue
			}

			stopCh, updateCh := d.ttlcallback(it.Key(), tm, *d.broadcastStopCh.Load())

			d.log.Debug("saving item with TTL", "key", it.Key(), "ttl", it.Timeout())
			d.heap.Set(it.Key(), &Item{
				key:     it.Key(),
				value:   it.Value(),
				timeout: it.Timeout(),
				callback: &cb{
					updateCh: updateCh,
					stopCh:   stopCh,
				},
			})
		} else {
			d.log.Debug("saving item without TTL", "key", it.Key())
			d.heap.Set(it.Key(), &Item{
				key:   it.Key(),
				value: it.Value(),
			})
		}
	}

	return nil
}

// MExpire sets the expiration time to the key
// If key already has the expiration time, it will be overwritten
func (d *Driver) MExpire(ctx context.Context, items ...kv.Item) error {
	const op = errors.Op("in_memory_plugin_mexpire")
	_, span := d.tracer.Tracer(tracerName).Start(ctx, "inmemory:mexpire")
	defer span.End()

	for _, it := range items {
		if it == nil {
			continue
		}

		if it.Timeout() == "" || strings.TrimSpace(it.Key()) == "" {
			span.RecordError(errors.Str("timeout for MExpire is empty or key is empty"))
			return errors.E(op, errors.Str("timeout for MExpire is empty or key is empty"))
		}

		tm, err := time.Parse(time.RFC3339, it.Timeout())
		if err != nil {
			span.RecordError(err)
			return errors.E(op, err)
		}

		ttm := int(tm.UTC().Sub(time.Now().UTC()).Seconds())
		if ttm <= 0 {
			d.log.Warn("incorrect TTL time for MExpire, saving without it", "key", it.Key())
			// Stop any existing TTL callback before overwriting so the old goroutine
			// cannot fire removeEntry on the newly saved item.
			d.heap.Delete(it.Key())
			d.heap.Set(it.Key(), &Item{
				key:   it.Key(),
				value: it.Value(),
			})
			continue
		}

		if clb, ok := d.heap.Get(it.Key()); ok && clb.callback != nil {
			clb.callback.updateCh <- ttm
		} else {
			stopCh, updateCh := d.ttlcallback(it.Key(), ttm, *d.broadcastStopCh.Load())
			d.heap.removeEntry(it.Key())
			d.heap.Set(it.Key(), &Item{
				key:     it.Key(),
				value:   it.Value(),
				timeout: it.Timeout(),
				callback: &cb{
					updateCh: updateCh,
					stopCh:   stopCh,
				},
			})
		}
	}

	return nil
}

func (d *Driver) TTL(ctx context.Context, keys ...string) (map[string]string, error) {
	const op = errors.Op("in_memory_plugin_ttl")
	_, span := d.tracer.Tracer(tracerName).Start(ctx, "inmemory:ttl")
	defer span.End()

	if len(keys) == 0 {
		span.RecordError(errors.Str("no keys provided"))
		return nil, errors.E(op, errors.NoKeys)
	}

	m := make(map[string]string, len(keys))
	for _, k := range keys {
		if strings.TrimSpace(k) == "" {
			span.RecordError(errors.Str("empty key"))
			return nil, errors.E(op, errors.EmptyKey)
		}

		if item, ok := d.heap.Get(k); ok {
			m[k] = item.Timeout()
		}
	}

	return m, nil
}

func (d *Driver) Delete(ctx context.Context, keys ...string) error {
	const op = errors.Op("in_memory_plugin_delete")
	_, span := d.tracer.Tracer(tracerName).Start(ctx, "inmemory:delete")
	defer span.End()

	if len(keys) == 0 {
		span.RecordError(errors.Str("no keys provided"))
		return errors.E(op, errors.NoKeys)
	}

	for _, key := range keys {
		if strings.TrimSpace(key) == "" {
			span.RecordError(errors.Str("empty key"))
			return errors.E(op, errors.EmptyKey)
		}
	}

	for _, key := range keys {
		d.heap.LoadAndDelete(key)
	}

	return nil
}

func (d *Driver) Clear(ctx context.Context) error {
	_, span := d.tracer.Tracer(tracerName).Start(ctx, "inmemory:clear")
	defer span.End()

	// stop all callbacks
	close(*d.broadcastStopCh.Load())

	newCh := make(chan struct{})
	d.broadcastStopCh.Swap(&newCh)
	d.heap.Clean()

	return nil
}

func (d *Driver) Stop(_ context.Context) {
	close(*d.broadcastStopCh.Load())
}

// ================================== PRIVATE ======================================

func (d *Driver) ttlcallback(id string, ttl int, sCh <-chan struct{}) (chan struct{}, chan int) {
	stopCbCh := make(chan struct{}, 1)
	updateTTLCh := make(chan int, 1)

	go func(hid string) {
		cbttl := ttl
		ta := time.NewTicker(time.Second * time.Duration(cbttl))
		for {
			select {
			case <-ta.C:
				d.log.Debug("ttl expired",
					"id", hid,
					"ttl seconds", cbttl,
				)
				ta.Stop()
				d.heap.removeEntry(hid)
				return
			case <-sCh:
				d.log.Debug("ttl removed, broadcast call",
					"id", hid,
					"ttl seconds", cbttl,
				)
				ta.Stop()
				d.heap.removeEntry(hid)
				return
			case <-stopCbCh:
				d.log.Debug("ttl removed, callback call",
					"id", hid,
					"ttl seconds", cbttl,
				)
				return
			case newTTL := <-updateTTLCh:
				d.log.Debug("updating ttl",
					"id", hid,
					"prev_ttl", cbttl,
					"new_ttl", newTTL)
				cbttl = newTTL
				ta.Reset(time.Second * time.Duration(newTTL))
			}
		}
	}(id)
	return stopCbCh, updateTTLCh
}
