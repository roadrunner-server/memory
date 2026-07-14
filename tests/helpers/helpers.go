package helpers

import (
	"net"
	"net/rpc"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v2"
	jobState "github.com/roadrunner-server/api-plugins/v6/jobs"
	goridgeRpc "github.com/roadrunner-server/goridge/v4/pkg/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	push    = "jobs.Push"
	pause   = "jobs.Pause"
	destroy = "jobs.Destroy"
	resume  = "jobs.Resume"
	stat    = "jobs.GetStats"
)

// dialRPC opens a goridge net/rpc client against the RoadRunner RPC endpoint.
func dialRPC(t *testing.T, address string) *rpc.Client {
	t.Helper()
	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", address)
	require.NoError(t, err)
	return rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))
}

// NewJobsClient returns a goridge net/rpc client for the jobs plugin RPC surface.
// The connection is closed on test cleanup.
func NewJobsClient(t *testing.T, address string) *rpc.Client {
	t.Helper()
	client := dialRPC(t, address)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// NewKVClient returns a goridge net/rpc client for the kv plugin RPC surface.
// The connection is closed on test cleanup.
func NewKVClient(t *testing.T, address string) *rpc.Client {
	t.Helper()
	client := dialRPC(t, address)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func ResumePipes(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := dialRPC(t, address)
		defer func() { _ = client.Close() }()

		err := client.Call(resume, &jobsProto.Pipelines{Pipelines: slices.Clone(pipes)}, &jobsProto.JobsHandlerResponse{})
		require.NoError(t, err)
	}
}

func PushToPipe(pipeline string, autoAck bool, address string) func(t *testing.T) {
	return func(t *testing.T) {
		client := dialRPC(t, address)
		defer func() { _ = client.Close() }()

		err := client.Call(push, &jobsProto.PushRequest{Job: createDummyJob(pipeline, autoAck)}, &jobsProto.JobsHandlerResponse{})
		require.NoError(t, err)
	}
}

func PushToDisabledPipe(address, pipeline string) func(t *testing.T) {
	return func(t *testing.T) {
		client := dialRPC(t, address)
		defer func() { _ = client.Close() }()

		req := &jobsProto.PushRequest{Job: &jobsProto.Job{
			Job:     "some/php/namespace",
			Id:      "1",
			Payload: []byte(`{"hello":"world"}`),
			Headers: nil,
			Options: &jobsProto.Options{
				Priority: 1,
				Pipeline: pipeline,
			},
		}}
		err := client.Call(push, req, &jobsProto.JobsHandlerResponse{})
		require.NoError(t, err)
	}
}

func PushToPipeDelayed(address string, pipeline string, delay int64) func(t *testing.T) {
	return func(t *testing.T) {
		client := dialRPC(t, address)
		defer func() { _ = client.Close() }()

		req := &jobsProto.PushRequest{Job: &jobsProto.Job{
			Job:     "some/php/namespace",
			Id:      uuid.NewString(),
			Payload: []byte(`{"hello":"world"}`),
			Headers: map[string]*jobsProto.JobHeaderValue{"test": {Values: []string{"test2"}}},
			Options: &jobsProto.Options{
				Priority: 1,
				Pipeline: pipeline,
				Delay:    delay,
			},
		}}
		err := client.Call(push, req, &jobsProto.JobsHandlerResponse{})
		assert.NoError(t, err)
	}
}

func createDummyJob(pipeline string, autoAck bool) *jobsProto.Job {
	return &jobsProto.Job{
		Job:     "some/php/namespace",
		Id:      uuid.NewString(),
		Payload: []byte(`{"hello":"world"}`),
		Headers: map[string]*jobsProto.JobHeaderValue{"test": {Values: []string{"test2"}}},
		Options: &jobsProto.Options{
			AutoAck:  autoAck,
			Priority: 1,
			Pipeline: pipeline,
			Topic:    pipeline,
		},
	}
}

func PausePipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := dialRPC(t, address)
		defer func() { _ = client.Close() }()

		err := client.Call(pause, &jobsProto.Pipelines{Pipelines: slices.Clone(pipes)}, &jobsProto.JobsHandlerResponse{})
		assert.NoError(t, err)
	}
}

func DestroyPipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := dialRPC(t, address)
		defer func() { _ = client.Close() }()

		req := &jobsProto.Pipelines{Pipelines: slices.Clone(pipes)}

		// Retry the destroy 10× with 1s gaps; if all attempts fail, return
		// without asserting. Some negative tests intentionally destroy
		// non-existent pipelines and rely on this silent-after-retry pattern.
		for range 10 {
			err := client.Call(destroy, req, &jobsProto.Pipelines{})
			if err == nil {
				return
			}
			time.Sleep(time.Second)
		}
	}
}

func Stats(address string, state *jobState.State) func(t *testing.T) {
	return func(t *testing.T) {
		client := dialRPC(t, address)
		defer func() { _ = client.Close() }()

		st := &jobsProto.Stats{}
		err := client.Call(stat, &emptypb.Empty{}, st)
		require.NoError(t, err)
		require.NotEmpty(t, st.GetStats())

		s := st.GetStats()[0]
		state.Queue = s.GetQueue()
		state.Pipeline = s.GetPipeline()
		state.Driver = s.GetDriver()
		state.Active = s.GetActive()
		state.Delayed = s.GetDelayed()
		state.Reserved = s.GetReserved()
		state.Ready = s.GetReady()
		state.Priority = s.GetPriority()
	}
}
