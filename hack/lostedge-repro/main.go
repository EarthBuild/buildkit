// lostedge-repro drives a real buildkitd into the solver failure behind
// "failed to get edge: inconsistent graph state" (moby/buildkit#2303,
// EarthBuild/buildkit#22), with no cancellation.
//
// Each iteration, with fresh build-context content:
//
//  1. warm: build a shared chain X (copy the context, then -depth RUN steps)
//     and target PA, which reads X through a read-only mount. PA's snapshot
//     does not depend on X's, so X's results can be pruned while PA's stays.
//  2. prune the results of X's steps from -keep onwards. Their cache keys stay
//     in the cache store, linked to PA.
//  3. race -pairs pairs of sessions. A builds PA again: a cache hit, so A's X
//     edges only compute cache keys, and A ends. B starts -gap later and builds
//     a new target PB on the same X, so X has to run. Each session names its
//     local context differently, so A and B load X under different vertex
//     digests with equal cache keys, and B's X edges merge into A's. When A
//     ends, its Discard deletes the states those merged edges still use, and
//     B fails looking one of them up.
//
// A build-cache GC that prunes step results and keeps their keys produces step
// 2 on a busy builder without help.
//
// Exit status: 0 when no B build failed, 1 when one did, 2 on a setup error.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/containerd/containerd/platforms"
	"github.com/moby/buildkit/client"
	"github.com/moby/buildkit/client/llb"
	gw "github.com/moby/buildkit/frontend/gateway/client"
	"github.com/tonistiigi/fsutil"
)

var (
	addr  = flag.String("addr", "tcp://127.0.0.1:1234", "buildkitd address")
	iters = flag.Int("iters", 8, "iterations")
	pairs = flag.Int("pairs", 3, "A/B session pairs raced per iteration")
	depth = flag.Int("depth", 10, "RUN steps in the shared chain (at most 10)")
	keep  = flag.Int("keep", 3, "leading chain steps whose results survive the prune")
	sleep = flag.String("sleep", "0.3", "seconds each chain step runs")
	gap   = flag.Duration("gap", 30*time.Millisecond, "B starts this long after A")
	noA   = flag.Bool("noA", false, "control: run B alone, without the cache-hit session A")
	image = flag.String("image", "docker.io/library/alpine:3.20", "base image")
)

var platform llb.ConstraintsOpt

// lostEdge matches the solver's lookups of a vertex state that is gone: the
// scheduler's getEdge and the slow-cache path's getState.
func lostEdge(err error) bool {
	if err == nil {
		return false
	}
	m := err.Error()
	for _, s := range []string{"inconsistent graph state", "failed to get state for index", "leaving outgoing open", "leaving incoming open"} {
		if strings.Contains(m, s) {
			return true
		}
	}
	return false
}

func chain(local string) llb.State {
	base := llb.Image(*image, platform)
	st := base.File(llb.Copy(llb.Local(local, llb.SharedKeyHint("ctx")), "/", "/src/", &llb.CopyInfo{CopyDirContentsOnly: true}))
	for i := 0; i < *depth; i++ {
		st = st.Run(llb.Shlex(fmt.Sprintf(`sh -c "sleep %s; echo s%d >> /src/h"`, *sleep, i))).Root()
	}
	return st
}

func target(local, tag string) llb.State {
	run := llb.Image(*image, platform).Run(llb.Shlex(fmt.Sprintf(`sh -c "cat /x/src/h > /out; echo %s >> /out"`, tag)))
	run.AddMount("/x", chain(local), llb.Readonly)
	return run.Root()
}

// build runs one session: a gateway build of target(tag), forced to execute.
func build(ctx context.Context, c *client.Client, dir, name, tag string) error {
	local := fmt.Sprintf("ctx-%s-%d", name, time.Now().UnixNano())
	fs, err := fsutil.NewFS(dir)
	if err != nil {
		return err
	}
	opt := client.SolveOpt{LocalMounts: map[string]fsutil.FS{local: fs}}
	_, err = c.Build(ctx, opt, "", func(ctx context.Context, g gw.Client) (*gw.Result, error) {
		def, err := target(local, tag).Marshal(ctx, platform)
		if err != nil {
			return nil, err
		}
		res, err := g.Solve(ctx, gw.SolveRequest{Definition: def.ToPB()})
		if err != nil {
			return nil, err
		}
		ref, err := res.SingleRef()
		if err != nil {
			return nil, err
		}
		_, err = ref.ReadDir(ctx, gw.ReadDirRequest{Path: "/"})
		return gw.NewResult(), err
	}, nil)
	return err
}

// prune removes the results of chain steps from -keep onwards. Prune filters do
// not see a record's description, so records are picked here and pruned by ID.
func prune(ctx context.Context, c *client.Client) (int, error) {
	re := regexp.MustCompile(fmt.Sprintf(`echo s[%d-9] >> /src/h`, *keep))
	du, err := c.DiskUsage(ctx)
	if err != nil {
		return 0, err
	}
	var ids []string
	for _, r := range du {
		if re.MatchString(r.Description) && !r.InUse {
			ids = append(ids, "id=="+r.ID)
		}
	}
	if len(ids) == 0 {
		return 0, nil
	}
	ch := make(chan client.UsageInfo)
	n := 0
	done := make(chan struct{})
	go func() {
		for range ch {
			n++
		}
		close(done)
	}()
	err = c.Prune(ctx, ch, client.WithFilter(ids))
	close(ch)
	<-done
	return n, err
}

func main() {
	flag.Parse()
	if *depth > 10 || *keep >= *depth {
		fmt.Fprintln(os.Stderr, "need -depth <= 10 and -keep < -depth")
		os.Exit(2)
	}
	ctx := context.Background()
	c, err := client.New(ctx, *addr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ws, err := c.ListWorkers(ctx)
	if err != nil || len(ws) == 0 || len(ws[0].Platforms) == 0 {
		fmt.Fprintln(os.Stderr, "listing workers:", err)
		os.Exit(2)
	}
	p := ws[0].Platforms[0]
	platform = llb.Platform(p)
	info, _ := c.Info(ctx)
	if info != nil {
		fmt.Printf("daemon %s %s (%s), platform %s\n", info.BuildkitVersion.Package, info.BuildkitVersion.Version, info.BuildkitVersion.Revision, platforms.Format(p))
	}

	dir, err := os.MkdirTemp("", "lostedge-repro")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer os.RemoveAll(dir)

	var totB, lostB, other int
	for it := 0; it < *iters; it++ {
		os.WriteFile(filepath.Join(dir, "a.txt"), []byte(fmt.Sprintf("iter %d %d\n", it, time.Now().UnixNano())), 0o644)
		if err := build(ctx, c, dir, "warm", "PA"); err != nil {
			fmt.Printf("iter %d: warm build failed: %v\n", it, err)
			other++
			continue
		}
		pruned, err := prune(ctx, c)
		if err != nil {
			fmt.Printf("iter %d: prune failed: %v\n", it, err)
		}

		var mu sync.Mutex
		var wg sync.WaitGroup
		res := ""
		for i := 0; i < *pairs; i++ {
			if !*noA {
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := build(ctx, c, dir, "A", "PA"); err != nil {
						mu.Lock()
						other++
						fmt.Printf("iter %d: A failed: %v\n", it, err)
						mu.Unlock()
					}
				}()
			}
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				time.Sleep(*gap)
				err := build(ctx, c, dir, "B", fmt.Sprintf("PB-%d-%d-%d", it, i, time.Now().UnixNano()))
				mu.Lock()
				defer mu.Unlock()
				totB++
				switch {
				case lostEdge(err):
					lostB++
					res += "F"
					fmt.Printf("iter %d: B failed: %v\n", it, err)
				case err != nil:
					other++
					res += "?"
					fmt.Printf("iter %d: B other error: %v\n", it, err)
				default:
					res += "."
				}
			}(i)
		}
		wg.Wait()
		fmt.Printf("iter %d: pruned %d records; B builds [%s]\n", it, pruned, res)
	}
	fmt.Printf("RESULT: %d of %d B builds lost an edge; %d other errors\n", lostB, totB, other)
	switch {
	case other > 0 && lostB == 0:
		os.Exit(2)
	case lostB > 0:
		os.Exit(1)
	}
}
