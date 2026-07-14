package ops

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	digest "github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

// Unconfigured, the feature must not exist: no coordinator, so ExecOp behaves
// exactly as upstream. A fork that changes behaviour when you did not ask for it
// is a fork nobody will merge.
func TestCoordinatorOffByDefault(t *testing.T) {
	t.Setenv("BUILDKIT_SINGLEFLIGHT_URL", "")
	require.Nil(t, coordinatorFromEnv())
}

func TestCoordinatorFromEnv(t *testing.T) {
	t.Setenv("BUILDKIT_SINGLEFLIGHT_URL", "http://127.0.0.1:5000")
	c := coordinatorFromEnv()
	require.NotNil(t, c)
	require.Equal(t, "cache", c.repo, "repo defaults rather than being required")
}

// The leader is told to build, and told so by a header rather than by guessing
// from an empty body.
func TestClaimLeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Contains(t, r.URL.Path, "/_rebuck/lease/claim/")
		w.Header().Set("X-Rebuck-Lease", "leader")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := &coordinator{base: srv.URL, repo: "cache", client: srv.Client()}
	pub, follower := c.claim(context.Background(), "abc")
	require.False(t, follower, "first claimant must build it")
	require.Nil(t, pub)
}

// A follower gets the leader's descriptor chain, and must preserve the per-output
// shape: edge.execOp indexes into the results.
func TestClaimFollowerGetsTheLeadersOutputs(t *testing.T) {
	want := publishedResult{Outputs: [][]ocispecs.Descriptor{
		{{Digest: digest.FromString("layer-a"), Size: 11}},
		nil, // a scratch output
	}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Rebuck-Lease", "follower")
		json.NewEncoder(w).Encode(want)
	}))
	defer srv.Close()

	c := &coordinator{base: srv.URL, repo: "cache", client: srv.Client()}
	pub, follower := c.claim(context.Background(), "abc")
	require.True(t, follower)
	require.Len(t, pub.Outputs, 2)
	require.Equal(t, want.Outputs[0][0].Digest, pub.Outputs[0][0].Digest)
	require.Empty(t, pub.Outputs[1], "the scratch output must survive the round trip")
}

// FAIL-OPEN is the whole safety story. A coordinator that is down, angry, or
// talking nonsense must make us BUILD, never block and never trust it. Duplicate
// work is always correct; a stall or a wrong layer is not.
func TestClaimFailsOpen(t *testing.T) {
	t.Run("unreachable", func(t *testing.T) {
		c := &coordinator{base: "http://127.0.0.1:1", repo: "cache", client: http.DefaultClient}
		_, follower := c.claim(context.Background(), "abc")
		require.False(t, follower, "an unreachable coordinator must degrade to plain buildkit")
	})

	t.Run("leader died (409)", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusConflict)
		}))
		defer srv.Close()
		c := &coordinator{base: srv.URL, repo: "cache", client: srv.Client()}
		_, follower := c.claim(context.Background(), "abc")
		require.False(t, follower, "re-claim is a rebuild, not a wait")
	})

	t.Run("unparseable result", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Rebuck-Lease", "follower")
			w.Write([]byte("this is not json"))
		}))
		defer srv.Close()
		c := &coordinator{base: srv.URL, repo: "cache", client: srv.Client()}
		_, follower := c.claim(context.Background(), "abc")
		require.False(t, follower, "rebuild rather than guess at what the leader meant")
	})
}

// worker.FromRemote calls Info() on every descriptor before touching the
// network. We answer from what the leader already told us — asking the registry
// again would be a round-trip to re-learn what we were just handed.
func TestDescriptorProviderInfoNeedsNoNetwork(t *testing.T) {
	d := ocispecs.Descriptor{Digest: digest.FromString("layer"), Size: 42}
	c := &coordinator{base: "http://never.dialed", repo: "cache", client: http.DefaultClient}
	rem := c.remoteFor([]ocispecs.Descriptor{d})

	info, err := rem.Provider.Info(context.Background(), d.Digest)
	require.NoError(t, err)
	require.Equal(t, int64(42), info.Size)
	require.Equal(t, d.Digest, info.Digest)
}

// A descriptor the leader never published must not resolve — that would be us
// inventing a layer.
func TestDescriptorProviderRejectsUnknownDigest(t *testing.T) {
	c := &coordinator{base: "http://never.dialed", repo: "cache", client: http.DefaultClient}
	rem := c.remoteFor([]ocispecs.Descriptor{{Digest: digest.FromString("known"), Size: 1}})

	_, err := rem.Provider.Info(context.Background(), digest.FromString("never published"))
	require.Error(t, err)
}

// The follower fetches layers straight from the coordinator's OCI registry —
// no RegistryHosts, no resolver, no session.
func TestBlobFetcherHitsTheRegistryBlobRoute(t *testing.T) {
	d := ocispecs.Descriptor{Digest: digest.FromString("layer"), Size: 5}
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.Path
		w.Write([]byte("hello"))
	}))
	defer srv.Close()

	f := &blobFetcher{base: srv.URL, repo: "cache", client: srv.Client()}
	rc, err := f.Fetch(context.Background(), d)
	require.NoError(t, err)
	defer rc.Close()
	require.Equal(t, "/v2/cache/blobs/"+d.Digest.String(), got)
}
