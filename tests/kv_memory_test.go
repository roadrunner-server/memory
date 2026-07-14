package memory

import (
	"log/slog"
	"maps"
	"os"
	"os/signal"
	"runtime"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	"tests/helpers"

	kvProto "github.com/roadrunner-server/api-go/v6/kv/v2"
	"github.com/roadrunner-server/config/v6"
	"github.com/roadrunner-server/endure/v2"
	"github.com/roadrunner-server/http/v6"
	"github.com/roadrunner-server/kv/v6"
	"github.com/roadrunner-server/logger/v6"
	"github.com/roadrunner-server/memory/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/roadrunner-server/server/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/protobuf/types/known/durationpb"
)

type kvInMemoryTracer struct {
	tp  *sdktrace.TracerProvider
	exp *tracetest.InMemoryExporter
}

func newKVInMemoryTracer(t *testing.T) *kvInMemoryTracer {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	t.Cleanup(func() { _ = tp.Shutdown(t.Context()) })
	return &kvInMemoryTracer{tp: tp, exp: exp}
}

func (m *kvInMemoryTracer) Init() error                      { return nil }
func (m *kvInMemoryTracer) Name() string                     { return "kvInMemoryTracer" }
func (m *kvInMemoryTracer) Tracer() *sdktrace.TracerProvider { return m.tp }

func TestInMemoryOrder(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2023.1.5",
		Path:    "configs/.rr-in-memory-order.yaml",
	}

	err := cont.RegisterAll(
		cfg,
		&kv.Plugin{},
		&memory.Plugin{},
		&http.Plugin{},
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	assert.NoError(t, err)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 1)
	stopCh <- struct{}{}
	wg.Wait()
}

func TestSetManyMemory(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2024.2.0",
		Path:    "configs/.rr-in-memory-memory.yaml",
	}

	err := cont.RegisterAll(
		cfg,
		&kv.Plugin{},
		&memory.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	assert.NoError(t, err)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 1)

	ms := &runtime.MemStats{}
	runtime.ReadMemStats(ms)
	prevAlloc := ms.Alloc
	ngprev := runtime.NumGoroutine()

	client := helpers.NewKVClient(t, "127.0.0.1:6666")

	tt := durationpb.New(time.Minute * 10)
	data := &kvProto.KvRequest{
		Storage: "memory-rr",
		Items: []*kvProto.KvItem{
			{Key: "a", Value: []byte("aa"), Ttl: tt},
			{Key: "b", Value: []byte("bb"), Ttl: tt},
			{Key: "c", Value: []byte("cc"), Ttl: tt},
			{Key: "d", Value: []byte("dd"), Ttl: tt},
		},
	}

	for range 10_000 {
		err := client.Call("kv.Set", data, &kvProto.KvResponse{})
		require.NoError(t, err)
	}
	runtime.GC()

	ms = &runtime.MemStats{}
	runtime.ReadMemStats(ms)
	currAlloc := ms.Alloc
	currNg := runtime.NumGoroutine()

	if currAlloc-prevAlloc > 20_000_000 { // 20MB
		t.Log("Prev alloc", prevAlloc)
		t.Log("Curr alloc", currAlloc)
		t.Error("Memory leak detected")
	}

	if currNg-ngprev > 10 {
		t.Log("Prev ng", ngprev)
		t.Log("Curr ng", currNg)
		t.Error("Goroutine leak detected")
	}

	time.Sleep(time.Second * 5)

	err = client.Call("kv.Clear", data, &kvProto.KvResponse{})
	require.NoError(t, err)

	stopCh <- struct{}{}
	wg.Wait()
}

func TestInMemory(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2023.3.0",
		Path:    "configs/.rr-in-memory.yaml",
	}

	err := cont.RegisterAll(
		cfg,
		&kv.Plugin{},
		&memory.Plugin{},
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	assert.NoError(t, err)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				// timeout
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second * 1)
	t.Run("INMEMORY", testRPCMethodsInMemory)
	stopCh <- struct{}{}
	wg.Wait()
}

func testRPCMethodsInMemory(t *testing.T) {
	const storage = "memory-rr"

	client := helpers.NewKVClient(t, "127.0.0.1:6001")

	tt := durationpb.New(time.Second * 5)
	keys := &kvProto.KvRequest{
		Storage: storage,
		Items: []*kvProto.KvItem{
			{Key: "a"},
			{Key: "b"},
			{Key: "c"},
		},
	}

	data := &kvProto.KvRequest{
		Storage: storage,
		Items: []*kvProto.KvItem{
			{Key: "a", Value: []byte("aa")},
			{Key: "b", Value: []byte("bb"), Ttl: durationpb.New(time.Second * 500)},
			{Key: "c", Value: []byte("cc"), Ttl: tt},
			{Key: "d", Value: []byte("dd")},
			{Key: "e", Value: []byte("ee")},
		},
	}

	err := client.Call("kv.Set", data, &kvProto.KvResponse{})
	assert.NoError(t, err)

	resp := &kvProto.KvResponse{}
	err = client.Call("kv.Has", keys, resp)
	assert.NoError(t, err)
	assert.Len(t, resp.GetItems(), 3)

	// key "c" should be deleted
	time.Sleep(time.Second * 7)

	resp = &kvProto.KvResponse{}
	err = client.Call("kv.Has", keys, resp)
	assert.NoError(t, err)
	assert.Len(t, resp.GetItems(), 2)

	resp = &kvProto.KvResponse{}
	err = client.Call("kv.MGet", keys, resp)
	assert.NoError(t, err)
	assert.Len(t, resp.GetItems(), 2) // c is expired

	tt2 := durationpb.New(time.Second * 10)

	data2 := &kvProto.KvRequest{
		Storage: storage,
		Items: []*kvProto.KvItem{
			{Key: "a", Ttl: tt2},
			{Key: "b", Ttl: tt2},
			{Key: "d", Ttl: tt2},
		},
	}

	err = client.Call("kv.MExpire", data2, &kvProto.KvResponse{})
	assert.NoError(t, err)

	keys2 := &kvProto.KvRequest{
		Storage: storage,
		Items: []*kvProto.KvItem{
			{Key: "a"},
			{Key: "b"},
			{Key: "d"},
		},
	}

	resp = &kvProto.KvResponse{}
	err = client.Call("kv.TTL", keys2, resp)
	assert.NoError(t, err)
	assert.Len(t, resp.GetItems(), 3)

	// HAS AFTER TTL
	time.Sleep(time.Second * 15)
	resp = &kvProto.KvResponse{}
	err = client.Call("kv.Has", keys2, resp)
	assert.NoError(t, err)
	assert.Empty(t, resp.GetItems())

	keysDel := &kvProto.KvRequest{
		Storage: storage,
		Items:   []*kvProto.KvItem{{Key: "e"}},
	}

	err = client.Call("kv.Delete", keysDel, &kvProto.KvResponse{})
	assert.NoError(t, err)

	resp = &kvProto.KvResponse{}
	err = client.Call("kv.Has", keysDel, resp)
	assert.NoError(t, err)
	assert.Empty(t, resp.GetItems())

	dataClear := &kvProto.KvRequest{
		Storage: storage,
		Items: []*kvProto.KvItem{
			{Key: "a", Value: []byte("aa")},
			{Key: "b", Value: []byte("bb")},
			{Key: "c", Value: []byte("cc")},
			{Key: "d", Value: []byte("dd")},
			{Key: "e", Value: []byte("ee")},
		},
	}

	err = client.Call("kv.Set", dataClear, &kvProto.KvResponse{})
	assert.NoError(t, err)

	resp = &kvProto.KvResponse{}
	err = client.Call("kv.Has", dataClear, resp)
	assert.NoError(t, err)
	assert.Len(t, resp.GetItems(), 5)

	err = client.Call("kv.Clear", &kvProto.KvRequest{Storage: storage}, &kvProto.KvResponse{})
	assert.NoError(t, err)

	resp = &kvProto.KvResponse{}
	err = client.Call("kv.Has", dataClear, resp)
	assert.NoError(t, err)
	assert.Empty(t, resp.GetItems())

	err = client.Call("kv.Clear", data, &kvProto.KvResponse{})
	require.NoError(t, err)
}

func TestInMemoryKVTracer(t *testing.T) {
	cont := endure.New(slog.LevelDebug)

	cfg := &config.Plugin{
		Version: "2023.3.0",
		Path:    "configs/.rr-in-memory.yaml",
	}

	tracer := newKVInMemoryTracer(t)
	err := cont.RegisterAll(
		cfg,
		&kv.Plugin{},
		&memory.Plugin{},
		tracer,
		&rpcPlugin.Plugin{},
		&logger.Plugin{},
	)
	assert.NoError(t, err)

	err = cont.Init()
	if err != nil {
		t.Fatal(err)
	}

	ch, err := cont.Serve()
	assert.NoError(t, err)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	wg := &sync.WaitGroup{}
	stopCh := make(chan struct{}, 1)

	wg.Go(func() {
		for {
			select {
			case e := <-ch:
				assert.Fail(t, "error", e.Error.Error())
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
			case <-sig:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			case <-stopCh:
				err = cont.Stop()
				if err != nil {
					assert.FailNow(t, "error", err.Error())
				}
				return
			}
		}
	})

	time.Sleep(time.Second)

	const storage = "memory-rr"

	client := helpers.NewKVClient(t, "127.0.0.1:6001")

	tt := durationpb.New(time.Second * 30)

	data := &kvProto.KvRequest{
		Storage: storage,
		Items: []*kvProto.KvItem{
			{Key: "a", Value: []byte("aa"), Ttl: tt},
			{Key: "b", Value: []byte("bb")},
		},
	}
	err = client.Call("kv.Set", data, &kvProto.KvResponse{})
	assert.NoError(t, err)

	keys := &kvProto.KvRequest{
		Storage: storage,
		Items:   []*kvProto.KvItem{{Key: "a"}, {Key: "b"}},
	}
	resp := &kvProto.KvResponse{}
	err = client.Call("kv.Has", keys, resp)
	assert.NoError(t, err)
	assert.Len(t, resp.GetItems(), 2)

	resp = &kvProto.KvResponse{}
	err = client.Call("kv.MGet", keys, resp)
	assert.NoError(t, err)
	assert.Len(t, resp.GetItems(), 2)

	resp = &kvProto.KvResponse{}
	err = client.Call("kv.TTL", &kvProto.KvRequest{
		Storage: storage,
		Items:   []*kvProto.KvItem{{Key: "a"}},
	}, resp)
	assert.NoError(t, err)
	assert.Len(t, resp.GetItems(), 1)

	tt2 := durationpb.New(time.Second * 60)
	err = client.Call("kv.MExpire", &kvProto.KvRequest{
		Storage: storage,
		Items:   []*kvProto.KvItem{{Key: "b", Ttl: tt2}},
	}, &kvProto.KvResponse{})
	assert.NoError(t, err)

	err = client.Call("kv.Delete", &kvProto.KvRequest{
		Storage: storage,
		Items:   []*kvProto.KvItem{{Key: "b"}},
	}, &kvProto.KvResponse{})
	assert.NoError(t, err)

	err = client.Call("kv.Clear", &kvProto.KvRequest{Storage: storage}, &kvProto.KvResponse{})
	assert.NoError(t, err)

	stopCh <- struct{}{}
	wg.Wait()

	// Verify spans
	spans := tracer.exp.GetSpans()
	spanNames := make(map[string]struct{}, len(spans))
	for _, s := range spans {
		spanNames[s.Name] = struct{}{}
	}

	uniqueNames := slices.Sorted(maps.Keys(spanNames))

	expected := []string{
		"inmemory:delete",
		"inmemory:has",
		"inmemory:mexpire",
		"inmemory:mget",
		"inmemory:set",
		"inmemory:ttl",
	}

	assert.Equal(t, expected, uniqueNames)
}
