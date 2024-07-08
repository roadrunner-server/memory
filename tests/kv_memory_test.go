package memory

import (
	"log/slog"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"

	kvProto "github.com/roadrunner-server/api/v4/build/kv/v1"
	"github.com/roadrunner-server/config/v5"
	"github.com/roadrunner-server/endure/v2"
	goridgeRpc "github.com/roadrunner-server/goridge/v3/pkg/rpc"
	"github.com/roadrunner-server/http/v5"
	"github.com/roadrunner-server/kv/v5"
	"github.com/roadrunner-server/logger/v5"
	"github.com/roadrunner-server/memory/v5"
	"github.com/roadrunner-server/otel/v5"
	rpcPlugin "github.com/roadrunner-server/rpc/v5"
	"github.com/roadrunner-server/server/v5"
	"github.com/stretchr/testify/assert"
)

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
	wg.Add(1)

	stopCh := make(chan struct{}, 1)

	go func() {
		defer wg.Done()
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
	}()

	time.Sleep(time.Second * 1)
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
		&otel.Plugin{},
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
	wg.Add(1)

	stopCh := make(chan struct{}, 1)

	go func() {
		defer wg.Done()
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
	}()

	time.Sleep(time.Second * 1)
	t.Run("INMEMORY", testRPCMethodsInMemory)
	stopCh <- struct{}{}
	wg.Wait()
}

func testRPCMethodsInMemory(t *testing.T) {
	conn, err := net.Dial("tcp", "127.0.0.1:6001")
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
}
