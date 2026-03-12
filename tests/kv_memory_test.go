package memory

import (
	"log/slog"
	"maps"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"runtime"
	"slices"
	"sync"
	"syscall"
	"testing"
	"time"

	kvProto "github.com/roadrunner-server/api-go/v6/kv/v1"
	"github.com/roadrunner-server/config/v5"
	"github.com/roadrunner-server/endure/v2"
	goridgeRpc "github.com/roadrunner-server/goridge/v4/pkg/rpc"
	"github.com/roadrunner-server/http/v5"
	"github.com/roadrunner-server/kv/v6"
	"github.com/roadrunner-server/logger/v5"
	"github.com/roadrunner-server/memory/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v5"
	"github.com/roadrunner-server/server/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
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

	t.Cleanup(func() {

	})
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

	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", "127.0.0.1:6666")
	assert.NoError(t, err)
	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))
	defer func() {
		_ = client.Close()
	}()

	tt := time.Now().Add(time.Minute * 10).Format(time.RFC3339)
	data := &kvProto.Request{
		Storage: "memory-rr",
		Items: []*kvProto.Item{
			{
				Key:     "a",
				Value:   []byte("aa"),
				Timeout: tt,
			},
			{
				Key:     "b",
				Value:   []byte("bb"),
				Timeout: tt,
			},
			{
				Key:     "c",
				Value:   []byte("cc"),
				Timeout: tt,
			},
			{
				Key:     "d",
				Value:   []byte("dd"),
				Timeout: tt,
			},
		},
	}

	ret := &kvProto.Response{}
	for range 10_000 {
		err = client.Call("kv.Set", data, ret)
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

	err = client.Call("kv.Clear", data, ret)
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
	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", "127.0.0.1:6001")
	assert.NoError(t, err)
	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))

	tt := time.Now().Add(time.Second * 5).Format(time.RFC3339)
	keys := &kvProto.Request{
		Storage: "memory-rr",
		Items: []*kvProto.Item{
			{
				Key: "a",
			},
			{
				Key: "b",
			},
			{
				Key: "c",
			},
		},
	}

	data := &kvProto.Request{
		Storage: "memory-rr",
		Items: []*kvProto.Item{
			{
				Key:   "a",
				Value: []byte("aa"),
			},
			{
				Key:     "b",
				Value:   []byte("bb"),
				Timeout: time.Now().Add(time.Second * 500).Format(time.RFC3339),
			},
			{
				Key:     "c",
				Value:   []byte("cc"),
				Timeout: tt,
			},
			{
				Key:   "d",
				Value: []byte("dd"),
			},
			{
				Key:   "e",
				Value: []byte("ee"),
			},
		},
	}

	ret := &kvProto.Response{}
	// Register 3 keys with values
	err = client.Call("kv.Set", data, ret)
	assert.NoError(t, err)

	ret = &kvProto.Response{}
	err = client.Call("kv.Has", keys, ret)
	assert.NoError(t, err)
	assert.Len(t, ret.GetItems(), 3) // should be 3

	// key "c" should be deleted
	time.Sleep(time.Second * 7)

	ret = &kvProto.Response{}
	err = client.Call("kv.Has", keys, ret)
	assert.NoError(t, err)
	assert.Len(t, ret.GetItems(), 2) // should be 2

	ret = &kvProto.Response{}
	err = client.Call("kv.MGet", keys, ret)
	assert.NoError(t, err)
	assert.Len(t, ret.GetItems(), 2) // c is expired

	tt2 := time.Now().Add(time.Second * 10).Format(time.RFC3339)

	data2 := &kvProto.Request{
		Storage: "memory-rr",
		Items: []*kvProto.Item{
			{
				Key:     "a",
				Timeout: tt2,
			},
			{
				Key:     "b",
				Timeout: tt2,
			},
			{
				Key:     "d",
				Timeout: tt2,
			},
		},
	}

	// MEXPIRE
	ret = &kvProto.Response{}
	err = client.Call("kv.MExpire", data2, ret)
	assert.NoError(t, err)

	// TTL
	keys2 := &kvProto.Request{
		Storage: "memory-rr",
		Items: []*kvProto.Item{
			{
				Key: "a",
			},
			{
				Key: "b",
			},
			{
				Key: "d",
			},
		},
	}

	ret = &kvProto.Response{}
	err = client.Call("kv.TTL", keys2, ret)
	assert.NoError(t, err)
	assert.Len(t, ret.GetItems(), 3)

	// HAS AFTER TTL
	time.Sleep(time.Second * 15)
	ret = &kvProto.Response{}
	err = client.Call("kv.Has", keys2, ret)
	assert.NoError(t, err)
	assert.Len(t, ret.GetItems(), 0)

	// DELETE
	keysDel := &kvProto.Request{
		Storage: "memory-rr",
		Items: []*kvProto.Item{
			{
				Key: "e",
			},
		},
	}

	ret = &kvProto.Response{}
	err = client.Call("kv.Delete", keysDel, ret)
	assert.NoError(t, err)

	// HAS AFTER DELETE
	ret = &kvProto.Response{}
	err = client.Call("kv.Has", keysDel, ret)
	assert.NoError(t, err)
	assert.Len(t, ret.GetItems(), 0)

	dataClear := &kvProto.Request{
		Storage: "memory-rr",
		Items: []*kvProto.Item{
			{
				Key:   "a",
				Value: []byte("aa"),
			},
			{
				Key:   "b",
				Value: []byte("bb"),
			},
			{
				Key:   "c",
				Value: []byte("cc"),
			},
			{
				Key:   "d",
				Value: []byte("dd"),
			},
			{
				Key:   "e",
				Value: []byte("ee"),
			},
		},
	}

	clr := &kvProto.Request{Storage: "memory-rr"}

	ret = &kvProto.Response{}
	// Register 3 keys with values
	err = client.Call("kv.Set", dataClear, ret)
	assert.NoError(t, err)

	ret = &kvProto.Response{}
	err = client.Call("kv.Has", dataClear, ret)
	assert.NoError(t, err)
	assert.Len(t, ret.GetItems(), 5) // should be 5

	ret = &kvProto.Response{}
	err = client.Call("kv.Clear", clr, ret)
	assert.NoError(t, err)

	ret = &kvProto.Response{}
	err = client.Call("kv.Has", dataClear, ret)
	assert.NoError(t, err)
	assert.Len(t, ret.GetItems(), 0) // should be 5

	err = client.Call("kv.Clear", data, ret)
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

	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", "127.0.0.1:6001")
	assert.NoError(t, err)
	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))

	tt := time.Now().Add(time.Second * 30).Format(time.RFC3339)

	// SET
	data := &kvProto.Request{
		Storage: "memory-rr",
		Items: []*kvProto.Item{
			{Key: "a", Value: []byte("aa"), Timeout: tt},
			{Key: "b", Value: []byte("bb")},
		},
	}
	ret := &kvProto.Response{}
	err = client.Call("kv.Set", data, ret)
	assert.NoError(t, err)

	// HAS
	keys := &kvProto.Request{
		Storage: "memory-rr",
		Items:   []*kvProto.Item{{Key: "a"}, {Key: "b"}},
	}
	ret = &kvProto.Response{}
	err = client.Call("kv.Has", keys, ret)
	assert.NoError(t, err)
	assert.Len(t, ret.GetItems(), 2)

	// MGET
	ret = &kvProto.Response{}
	err = client.Call("kv.MGet", keys, ret)
	assert.NoError(t, err)
	assert.Len(t, ret.GetItems(), 2)

	// TTL
	ret = &kvProto.Response{}
	err = client.Call("kv.TTL", &kvProto.Request{
		Storage: "memory-rr",
		Items:   []*kvProto.Item{{Key: "a"}},
	}, ret)
	assert.NoError(t, err)
	assert.Len(t, ret.GetItems(), 1)

	// MEXPIRE
	tt2 := time.Now().Add(time.Second * 60).Format(time.RFC3339)
	ret = &kvProto.Response{}
	err = client.Call("kv.MExpire", &kvProto.Request{
		Storage: "memory-rr",
		Items:   []*kvProto.Item{{Key: "b", Timeout: tt2}},
	}, ret)
	assert.NoError(t, err)

	// DELETE
	ret = &kvProto.Response{}
	err = client.Call("kv.Delete", &kvProto.Request{
		Storage: "memory-rr",
		Items:   []*kvProto.Item{{Key: "b"}},
	}, ret)
	assert.NoError(t, err)

	// CLEAR
	ret = &kvProto.Response{}
	err = client.Call("kv.Clear", &kvProto.Request{Storage: "memory-rr"}, ret)
	assert.NoError(t, err)

	_ = client.Close()

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
