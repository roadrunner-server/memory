package memory

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	kvProto "github.com/roadrunner-server/api-go/v6/kv/v2"
	"github.com/roadrunner-server/api-go/v6/kv/v2/kvV2connect"
	"github.com/stretchr/testify/require"
)

// fakeKvService stubs only Has — the procedure the PHP worker
// `kv-order.php` would have invoked via spiral/goridge to probe whether
// "test_key" is present in the "memory-rr" storage. Other methods fall
// through to UnimplementedKvServiceHandler (CodeUnimplemented).
type fakeKvService struct {
	kvV2connect.UnimplementedKvServiceHandler
}

func (fakeKvService) Has(
	_ context.Context, _ *connect.Request[kvProto.KvRequest],
) (*connect.Response[kvProto.KvResponse], error) {
	// Empty Items mirrors what an empty memory storage would have returned
	// for the PHP worker's startup get("test_key") probe.
	return connect.NewResponse(&kvProto.KvResponse{}), nil
}

// TestKVNativeHas is a pure request/response Connect-RPC smoke test for
// kv.KvService.Has, mirroring what the (still-broken) PHP TestInMemoryOrder
// path exercises. No Roadrunner container, no PHP — just proves the proto
// types + connectrpc wire round-trip for this procedure.
func TestKVNativeHas(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle(kvV2connect.NewKvServiceHandler(fakeKvService{}))

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	client := kvV2connect.NewKvServiceClient(srv.Client(), strings.TrimSuffix(srv.URL, "/"))
	resp, err := client.Has(t.Context(), connect.NewRequest(&kvProto.KvRequest{
		Storage: "memory-rr",
		Items:   []*kvProto.KvItem{{Key: "test_key"}},
	}))
	require.NoError(t, err)
	require.Empty(t, resp.Msg.GetItems())
}
