package helpers

import (
	"net"
	"net/rpc"
	"testing"
	"time"

	"github.com/google/uuid"
	jobsProto "github.com/roadrunner-server/api/v4/build/jobs/v1"
	jobState "github.com/roadrunner-server/api/v4/plugins/v1/jobs"
	goridgeRpc "github.com/roadrunner-server/goridge/v3/pkg/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	push    string = "jobs.Push"
	pause   string = "jobs.Pause"
	destroy string = "jobs.Destroy"
	resume  string = "jobs.Resume"
	stat    string = "jobs.Stat"
)

func ResumePipes(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		conn, err := net.Dial("tcp", address)
		require.NoError(t, err)
		defer func() {
			_ = conn.Close()
		}()
		client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))

		pipe := &jobsProto.Pipelines{Pipelines: make([]string, len(pipes))}

		for i := 0; i < len(pipes); i++ {
			pipe.GetPipelines()[i] = pipes[i]
		}

		er := &jobsProto.Empty{}
		err = client.Call(resume, pipe, er)
		require.NoError(t, err)
	}
}

func PushToPipe(pipeline string, autoAck bool, address string) func(t *testing.T) {
	return func(t *testing.T) {
		conn, err := net.Dial("tcp", address)
		require.NoError(t, err)
		defer func() {
			_ = conn.Close()
		}()
		client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))

		req := &jobsProto.PushRequest{Job: createDummyJob(pipeline, autoAck)}

		er := &jobsProto.Empty{}
		err = client.Call(push, req, er)
		require.NoError(t, err)
	}
}

func PausePipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		conn, err := net.Dial("tcp", address)
		require.NoError(t, err)
		defer func() {
			_ = conn.Close()
		}()
		client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))

		pipe := &jobsProto.Pipelines{Pipelines: make([]string, len(pipes))}

		for i := 0; i < len(pipes); i++ {
			pipe.GetPipelines()[i] = pipes[i]
		}

		er := &jobsProto.Empty{}
		err = client.Call(pause, pipe, er)
		assert.NoError(t, err)
	}
}

func DestroyPipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		conn, err := net.Dial("tcp", address)
		require.NoError(t, err)
		defer func() {
			_ = conn.Close()
		}()
		client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))

		pipe := &jobsProto.Pipelines{Pipelines: make([]string, len(pipes))}

		for i := 0; i < len(pipes); i++ {
			pipe.GetPipelines()[i] = pipes[i]
		}

		for i := 0; i < 10; i++ {
			er := &jobsProto.Empty{}
			err = client.Call(destroy, pipe, er)
			if err != nil {
				time.Sleep(time.Second)
				continue
			}
			assert.NoError(t, err)
			break
		}
	}
}

func Stats(address string, state *jobState.State) func(t *testing.T) {
	return func(t *testing.T) {
		conn, err := net.Dial("tcp", address)
		require.NoError(t, err)
		defer func() {
			_ = conn.Close()
		}()
		client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))

		st := &jobsProto.Stats{}
		er := &jobsProto.Empty{}

		err = client.Call(stat, er, st)
		require.NoError(t, err)
		require.NotNil(t, st)

		state.Queue = st.Stats[0].Queue
		state.Pipeline = st.Stats[0].Pipeline
		state.Driver = st.Stats[0].Driver
		state.Active = st.Stats[0].Active
		state.Delayed = st.Stats[0].Delayed
		state.Reserved = st.Stats[0].Reserved
		state.Ready = st.Stats[0].Ready
		state.Priority = st.Stats[0].Priority
	}
}

func PushToDisabledPipe(address, pipeline string) func(t *testing.T) {
	return func(t *testing.T) {
		conn, err := net.Dial("tcp", address)
		require.NoError(t, err)
		client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))

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

		er := &jobsProto.Empty{}
		err = client.Call(push, req, er)
		require.NoError(t, err)
	}
}

func PushToPipeDelayed(address string, pipeline string, delay int64) func(t *testing.T) {
	return func(t *testing.T) {
		conn, err := net.Dial("tcp", address)
		assert.NoError(t, err)
		client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))

		req := &jobsProto.PushRequest{Job: &jobsProto.Job{
			Job:     "some/php/namespace",
			Id:      uuid.NewString(),
			Payload: []byte(`{"hello":"world"}`),
			Headers: map[string]*jobsProto.HeaderValue{"test": {Value: []string{"test2"}}},
			Options: &jobsProto.Options{
				Priority: 1,
				Pipeline: pipeline,
				Delay:    delay,
			},
		}}

		er := &jobsProto.Empty{}
		err = client.Call(push, req, er)
		assert.NoError(t, err)
	}
}

func createDummyJob(pipeline string, autoAck bool) *jobsProto.Job {
	return &jobsProto.Job{
		Job:     "some/php/namespace",
		Id:      uuid.NewString(),
		Payload: []byte(`{"hello":"world"}`),
		Headers: map[string]*jobsProto.HeaderValue{"test": {Value: []string{"test2"}}},
		Options: &jobsProto.Options{
			AutoAck:  autoAck,
			Priority: 1,
			Pipeline: pipeline,
			Topic:    pipeline,
		},
	}
}
