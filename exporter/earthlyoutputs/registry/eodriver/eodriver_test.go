package eodriver

import (
	"context"
	"testing"

	storagedriver "github.com/docker/distribution/registry/storage/driver"
	digest "github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

// Unknown content must be reported as not found, so that the registry answers
// 404 rather than 500, e.g. when used as a mirror.
func TestGetUnknownIsPathNotFound(t *testing.T) {
	d := &driver{mmp: NewMultiMultiProvider()}
	sha := digest.FromString("unknown").Encoded()

	for _, path := range []string{
		"/docker/registry/v2/repositories/library/alpine/_manifests/tags/latest/current/link",
		"/docker/registry/v2/blobs/sha256/" + sha[:2] + "/" + sha + "/data",
	} {
		_, _, err := d.get(context.Background(), path, 0)
		require.ErrorAs(t, err, &storagedriver.PathNotFoundError{}, path)
	}
}
