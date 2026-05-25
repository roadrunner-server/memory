package memory

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v2"
	"github.com/roadrunner-server/api-go/v6/jobs/v2/jobsV2connect"
	"github.com/stretchr/testify/require"
)

// fakeJobsService stubs only Declare — the procedure the PHP worker
// `jobs_create_memory.php` would have invoked via spiral/goridge to register
// a new in-memory pipeline at runtime. Other methods fall through to
// UnimplementedJobsServiceHandler (CodeUnimplemented).
type fakeJobsService struct {
	jobsV2connect.UnimplementedJobsServiceHandler
}

func (fakeJobsService) Declare(
	_ context.Context, _ *connect.Request[jobsProto.DeclareRequest],
) (*connect.Response[jobsProto.JobsHandlerResponse], error) {
	return connect.NewResponse(&jobsProto.JobsHandlerResponse{}), nil
}

// TestJobsNativeDeclare is a pure request/response Connect-RPC smoke test for
// jobs.JobsService.Declare, mirroring what the (still-broken) PHP
// TestMemoryCreate exercises via Jobs.create(MemoryCreateInfo). No
// Roadrunner container, no PHP — just proves the proto types + connectrpc
// wire round-trip for this procedure.
func TestJobsNativeDeclare(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle(jobsV2connect.NewJobsServiceHandler(fakeJobsService{}))

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := jobsV2connect.NewJobsServiceClient(srv.Client(), strings.TrimSuffix(srv.URL, "/"))
	_, err := client.Declare(t.Context(), connect.NewRequest(&jobsProto.DeclareRequest{
		Pipeline: map[string]string{
			"driver":   "memory",
			"name":     "example",
			"priority": "10",
			"prefetch": "100",
		},
	}))
	require.NoError(t, err)
}
