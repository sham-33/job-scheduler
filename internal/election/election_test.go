package election

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sham-33/job-scheduler/internal/store"
	"github.com/sham-33/job-scheduler/internal/store/storetest"
)

// Short timings keep the tests fast. The ratios match the defaults.
const (
	ttl   = 300 * time.Millisecond
	renew = 100 * time.Millisecond
	poll  = 20 * time.Millisecond
)

func newElector(rdb *redis.Client, keys store.Keys, id string) *Elector {
	return New(rdb, keys, Options{ID: id, TTL: ttl, RenewEvery: renew, PollEvery: poll}, slog.New(slog.DiscardHandler))
}

// term is one call of lead: one candidate's time as leader.
type term struct {
	id         string
	epoch      int64
	start, end time.Time
}

// campaign runs e until the test ends or the returned stop is called, and
// reports each term on starts when it begins and on ends when it is over.
func campaign(t *testing.T, e *Elector, starts, ends chan<- term) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.Run(ctx, func(ctx context.Context, epoch int64) {
			tm := term{id: e.opts.ID, epoch: epoch, start: time.Now()}
			starts <- tm
			<-ctx.Done()
			tm.end = time.Now()
			ends <- tm
		})
	}()
	stop = sync.OnceFunc(func() {
		cancel()
		<-done
	})
	t.Cleanup(stop)
	return stop
}

func terms() chan term { return make(chan term, 16) }

func waitTerm(t *testing.T, ch <-chan term, within time.Duration) term {
	t.Helper()
	select {
	case tm := <-ch:
		return tm
	case <-time.After(within):
		t.Fatalf("no term within %s", within)
		return term{}
	}
}

func noTerm(t *testing.T, ch <-chan term, during time.Duration) {
	t.Helper()
	select {
	case tm := <-ch:
		t.Fatalf("unexpected term %+v", tm)
	case <-time.After(during):
	}
}

func mustLeader(t *testing.T, rdb *redis.Client, keys store.Keys) (string, int64) {
	t.Helper()
	id, epoch, err := Leader(context.Background(), rdb, keys)
	if err != nil {
		t.Fatalf("Leader: %v", err)
	}
	return id, epoch
}

func TestScripts(t *testing.T) {
	rdb := storetest.Client(t)
	keys := storetest.Keys(t, rdb)
	ctx := context.Background()
	a, b := newElector(rdb, keys, "a"), newElector(rdb, keys, "b")

	if id, epoch := mustLeader(t, rdb, keys); id != "" || epoch != 0 {
		t.Errorf("Leader before any election = %q, %d, want none", id, epoch)
	}
	if epoch, err := a.acquire(ctx); err != nil || epoch != 1 {
		t.Fatalf("a.acquire = %d, %v, want epoch 1", epoch, err)
	}
	if left := rdb.PTTL(ctx, keys.Leader()).Val(); left <= 0 || left > ttl {
		t.Errorf("seat expires in %s, want within the %s term", left, ttl)
	}
	if id, epoch := mustLeader(t, rdb, keys); id != "a" || epoch != 1 {
		t.Errorf("Leader = %q, %d, want a, 1", id, epoch)
	}

	// Another candidate can't take, renew or release a's seat.
	if epoch, err := b.acquire(ctx); err != nil || epoch != 0 {
		t.Errorf("b.acquire = %d, %v, want 0 while a leads", epoch, err)
	}
	if ok, err := b.renew(ctx); err != nil || ok {
		t.Errorf("b.renew = %v, %v, want false", ok, err)
	}
	if err := b.release(ctx); err != nil {
		t.Fatalf("b.release: %v", err)
	}
	if id, _ := mustLeader(t, rdb, keys); id != "a" {
		t.Errorf("after b's release the leader is %q, want a", id)
	}

	// a can take its own seat back, as a new term, after giving it up
	// locally when a renewal failed on its side.
	if epoch, err := a.acquire(ctx); err != nil || epoch != 2 {
		t.Errorf("a.acquire of its own seat = %d, %v, want epoch 2", epoch, err)
	}
	if ok, err := a.renew(ctx); err != nil || !ok {
		t.Errorf("a.renew = %v, %v, want true", ok, err)
	}
	if err := a.release(ctx); err != nil {
		t.Fatalf("a.release: %v", err)
	}
	if id, epoch := mustLeader(t, rdb, keys); id != "" || epoch != 2 {
		t.Errorf("after release Leader = %q, %d, want none, 2", id, epoch)
	}
	if ok, err := a.renew(ctx); err != nil || ok {
		t.Errorf("a.renew after release = %v, %v, want false", ok, err)
	}
}

func TestSoleCandidateLeadsUntilStopped(t *testing.T) {
	rdb := storetest.Client(t)
	keys := storetest.Keys(t, rdb)
	starts, ends := terms(), terms()
	stop := campaign(t, newElector(rdb, keys, "a"), starts, ends)

	if tm := waitTerm(t, starts, time.Second); tm.epoch != 1 {
		t.Errorf("first term has epoch %d, want 1", tm.epoch)
	}
	// Renewals keep it leader well past one term.
	noTerm(t, ends, 3*ttl)
	if id, epoch := mustLeader(t, rdb, keys); id != "a" || epoch != 1 {
		t.Errorf("Leader = %q, %d, want a, 1", id, epoch)
	}

	stop()
	select {
	case <-ends:
	default:
		t.Error("Run returned before the term ended")
	}
	if id, _ := mustLeader(t, rdb, keys); id != "" {
		t.Errorf("Leader after stop = %q, want the seat given up", id)
	}
}

func TestOneLeaderAtATimeAndQuickHandover(t *testing.T) {
	rdb := storetest.Client(t)
	keys := storetest.Keys(t, rdb)
	starts, ends := terms(), terms()
	stops := map[string]func(){}
	for _, id := range []string{"a", "b", "c"} {
		stops[id] = campaign(t, newElector(rdb, keys, id), starts, ends)
	}

	first := waitTerm(t, starts, time.Second)
	noTerm(t, starts, 3*ttl) // the others wait

	stops[first.id]() // graceful: it hands the seat over
	ended := waitTerm(t, ends, time.Second)
	second := waitTerm(t, starts, ttl)
	gap := second.start.Sub(ended.end)
	t.Logf("%s took over %s after %s stepped down (term %s)", second.id, gap.Round(time.Millisecond), first.id, ttl)
	if second.id == first.id || second.epoch != first.epoch+1 {
		t.Errorf("second term = %+v, want another candidate with epoch %d", second, first.epoch+1)
	}
	if gap < 0 || gap > ttl/2 {
		t.Errorf("handover took %s, want well under the %s term (the seat is released, not left to expire)", gap, ttl)
	}
	noTerm(t, starts, 3*ttl)
}

func TestCrashedLeaderIsReplacedWhenItsTermExpires(t *testing.T) {
	rdb := storetest.Client(t)
	keys := storetest.Keys(t, rdb)
	ctx := context.Background()
	// A leader that renewed one last time and then died.
	rdb.Set(ctx, keys.Leader(), "crashed", ttl)
	rdb.Incr(ctx, keys.Epoch())
	crashed := time.Now()

	starts, ends := terms(), terms()
	campaign(t, newElector(rdb, keys, "b"), starts, ends)
	tm := waitTerm(t, starts, 3*ttl)
	took := tm.start.Sub(crashed)
	t.Logf("took over %s after the crashed leader's last renewal (term %s)", took.Round(time.Millisecond), ttl)
	if took < ttl-50*time.Millisecond || took > ttl+poll+100*time.Millisecond {
		t.Errorf("took over after %s, want about the %s term", took, ttl)
	}
	if tm.epoch != 2 {
		t.Errorf("epoch = %d, want 2", tm.epoch)
	}
}

func TestLeaderCutOffFromRedisStepsDownBeforeTheNextOneStarts(t *testing.T) {
	rdb := storetest.Client(t)
	keys := storetest.Keys(t, rdb)
	rdbA := storetest.Client(t) // a's own connection, so it can be cut off alone

	startsA, endsA := terms(), terms()
	campaign(t, newElector(rdbA, keys, "a"), startsA, endsA)
	waitTerm(t, startsA, time.Second)
	startsB, endsB := terms(), terms()
	campaign(t, newElector(rdb, keys, "b"), startsB, endsB)
	noTerm(t, startsB, 2*ttl)

	cut := time.Now()
	_ = rdbA.Close()
	endA := waitTerm(t, endsA, 3*renew)
	startB := waitTerm(t, startsB, 2*ttl)
	t.Logf("a stopped %s after the cut; b started %s after a stopped",
		endA.end.Sub(cut).Round(time.Millisecond), startB.start.Sub(endA.end).Round(time.Millisecond))
	if !startB.start.After(endA.end) {
		t.Errorf("b started at %s, before a stopped at %s", startB.start, endA.end)
	}
	if startB.epoch != 2 {
		t.Errorf("b's epoch = %d, want 2", startB.epoch)
	}
}

func TestLeaderStepsDownWhenAnotherHoldsTheSeat(t *testing.T) {
	rdb := storetest.Client(t)
	keys := storetest.Keys(t, rdb)
	ctx := context.Background()
	starts, ends := terms(), terms()
	campaign(t, newElector(rdb, keys, "a"), starts, ends)
	waitTerm(t, starts, time.Second)

	// Something else now holds the seat, such as a scheduler that took over
	// while a was paused.
	rdb.Set(ctx, keys.Leader(), "other", 0)
	waitTerm(t, ends, 3*renew)
	noTerm(t, starts, 3*ttl) // it waits while the seat is taken

	rdb.Del(ctx, keys.Leader())
	if tm := waitTerm(t, starts, ttl); tm.epoch != 2 {
		t.Errorf("epoch after taking the seat back = %d, want 2", tm.epoch)
	}
}

func TestLeadReturningEarlyEndsTheTerm(t *testing.T) {
	rdb := storetest.Client(t)
	keys := storetest.Keys(t, rdb)
	e := newElector(rdb, keys, "a")
	epochs := make(chan int64, 4)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		calls := 0
		e.Run(ctx, func(ctx context.Context, epoch int64) {
			epochs <- epoch
			if calls++; calls > 1 {
				<-ctx.Done() // the second term does its work properly
			}
		})
	}()
	defer func() {
		cancel()
		<-done
	}()

	for want := int64(1); want <= 2; want++ {
		select {
		case got := <-epochs:
			if got != want {
				t.Fatalf("term epoch = %d, want %d", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("no term %d: a term whose work stopped was not ended", want)
		}
	}
}
