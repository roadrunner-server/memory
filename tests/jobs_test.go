package memory

import (
	"testing"

	"tests/helpers"

	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v2"
	jobState "github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/roadrunner-server/informer/v6"
	"github.com/roadrunner-server/jobs/v6"
	memoryPlugin "github.com/roadrunner-server/memory/v6"
	"github.com/roadrunner-server/resetter/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/roadrunner-server/server/v6"
	"github.com/stretchr/testify/require"
)

const (
	rpcAddr  = "127.0.0.1:6001"
	pipeline = "test-3"
)

func jobsPlugins() []any {
	return []any{
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&memoryPlugin.Plugin{},
	}
}

// bootJobs starts the container with the observed logger and waits for the rpc
// listener, which is the readiness signal the fixed sleeps used to stand in for.
func bootJobs(t *testing.T, cfgPath string) (*helpers.RR, func()) {
	t.Helper()

	return helpers.Start(t, cfgPath, jobsPlugins(),
		helpers.WithObservedLogger(),
		helpers.WithTCPProbe(rpcAddr),
	)
}

// declarePipe declares the memory pipeline the tests push to.
func declarePipe(t *testing.T, prefetch string) {
	t.Helper()

	client := helpers.NewJobsClient(t, rpcAddr)
	req := &jobsProto.DeclareRequest{Pipeline: map[string]string{
		"driver":   "memory",
		"name":     pipeline,
		"prefetch": prefetch,
		"priority": "33",
	}}

	require.NoError(t, client.Call("jobs.Declare", req, &jobsProto.JobsHandlerResponse{}))
}

// consumePipe resumes consumption on the declared pipeline.
func consumePipe(t *testing.T) {
	t.Helper()

	client := helpers.NewJobsClient(t, rpcAddr)
	require.NoError(t, client.Call("jobs.Resume",
		&jobsProto.Pipelines{Pipelines: []string{pipeline}},
		&jobsProto.JobsHandlerResponse{}))
}

// TestBoots covers the plain init config.
func TestBoots(t *testing.T) {
	rr, _ := bootJobs(t, "configs/.rr-memory-init.yaml")

	rr.WaitLog(t, "plugin was started", 1)
}

// TestPushAndProcess declares a pipeline, pushes one job and follows it through
// to completion, waiting on the records rather than sleeping between steps.
func TestPushAndProcess(t *testing.T) {
	rr, _ := bootJobs(t, "configs/.rr-memory-declare.yaml")

	declarePipe(t, "10000")
	consumePipe(t)

	helpers.PushToPipe(pipeline, false, rpcAddr)(t)

	rr.WaitLog(t, "job was pushed successfully", 1)
	rr.WaitLog(t, "job processing was started", 1)
	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.PausePipelines(rpcAddr, pipeline)(t)
	rr.WaitLog(t, "pipeline was paused", 1)

	helpers.DestroyPipelines(rpcAddr, pipeline)(t)

	rr.RequireLogCount(t, "job was pushed successfully", 1)
	rr.RequireLogCount(t, "job was processed successfully", 1)
}

// TestPauseResume pauses a pipeline the config consumes at startup, checks a
// push to it is rejected while paused, then resumes and pushes again.
func TestPauseResume(t *testing.T) {
	const pipe = "test-local"

	rr, _ := bootJobs(t, "configs/.rr-memory-pause-resume.yaml")

	helpers.PausePipelines(rpcAddr, pipe)(t)
	rr.WaitLog(t, "pipeline was paused", 1)

	helpers.PushToDisabledPipe(rpcAddr, pipe)(t)

	helpers.ResumePipes(rpcAddr, pipe)(t)
	rr.WaitLog(t, "pipeline was resumed", 1)

	helpers.PushToPipe(pipe, false, rpcAddr)(t)
	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.DestroyPipelines(rpcAddr, pipe)(t)

	rr.RequireLogCount(t, "pipeline was resumed", 1)
}

// TestStatsReportDelayedAndDrained pushes a delayed job and a plain one, then
// polls the pipeline state instead of sleeping out the delay.
func TestStatsReportDelayedAndDrained(t *testing.T) {
	rr, _ := bootJobs(t, "configs/.rr-memory-declare.yaml")

	declarePipe(t, "10000")
	consumePipe(t)

	helpers.PushToPipe(pipeline, false, rpcAddr)(t)
	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.PausePipelines(rpcAddr, pipeline)(t)
	rr.WaitLog(t, "pipeline was paused", 1)

	// with consumption paused, a delayed job stays counted as delayed
	helpers.PushToPipeDelayed(rpcAddr, pipeline, 2)(t)
	helpers.PushToPipe(pipeline, false, rpcAddr)(t)

	delayed := helpers.WaitStats(t, rpcAddr, func(s *jobState.State) bool {
		return s.Delayed == 1
	})

	require.Equal(t, pipeline, delayed.Pipeline)
	require.Equal(t, "memory", delayed.Driver)
	require.Equal(t, pipeline, delayed.Queue)
	require.Equal(t, uint64(33), delayed.Priority)

	// resuming drains both the queued and the delayed job once its delay lapses
	consumePipe(t)

	drained := helpers.WaitStats(t, rpcAddr, func(s *jobState.State) bool {
		return s.Delayed == 0 && s.Active == 0 && s.Reserved == 0
	})

	require.Equal(t, pipeline, drained.Pipeline)
	require.Equal(t, uint64(33), drained.Priority)

	helpers.DestroyPipelines(rpcAddr, pipeline)(t)
}

// TestPrefetchLimit declares a pipeline with prefetch 1 and pushes ten jobs, so
// the driver has to hold jobs back until the in-flight one finishes. The old
// test waited out a flat 15s; this waits for the tenth job to be processed.
func TestPrefetchLimit(t *testing.T) {
	const jobCount = 10

	rr, stop := bootJobs(t, "configs/.rr-memory-prefetch.yaml")

	declarePipe(t, "1")
	consumePipe(t)

	for range jobCount {
		helpers.PushToPipe(pipeline, false, rpcAddr)(t)
	}

	rr.WaitLog(t, "job was processed successfully", jobCount)
	rr.WaitLog(t, "prefetch limit was reached, waiting for the jobs to be processed", 1)

	helpers.DestroyPipelines(rpcAddr, pipeline)(t)

	rr.RequireLogCount(t, "job was pushed successfully", jobCount)
	rr.RequireLogCount(t, "job was processed successfully", jobCount)

	// the destroy record is written while the container shuts down
	stop()
	rr.WaitLog(t, "destroy signal received", 1)
}

// TestProtocolErrorIsReported covers a worker that answers with something the
// jobs protocol cannot parse. The old test slept 25s waiting for the error.
func TestProtocolErrorIsReported(t *testing.T) {
	rr, _ := bootJobs(t, "configs/.rr-memory-jobs-err.yaml")

	declarePipe(t, "10000")
	helpers.ResumePipes(rpcAddr, pipeline)(t)
	helpers.PushToPipe(pipeline, false, rpcAddr)(t)

	rr.WaitLog(t, "jobs protocol error", 1)

	helpers.PausePipelines(rpcAddr, pipeline)(t)
	helpers.DestroyPipelines(rpcAddr, pipeline)(t)
}

// TestResponseHandlerError pushes to two pipelines whose worker answers with a
// payload the response handler cannot parse, so each push produces one error.
func TestResponseHandlerError(t *testing.T) {
	rr, _ := bootJobs(t, "configs/.rr-memory-init-v27-br.yaml")

	helpers.PushToPipe("test-1", false, rpcAddr)(t)
	helpers.PushToPipe("test-2", false, rpcAddr)(t)

	rr.WaitLog(t, "response handler error", 2)

	helpers.DestroyPipelines(rpcAddr, "test-1", "test-2")(t)

	rr.RequireLogCount(t, "response handler error", 2)
}
