package queue

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sham-33/job-scheduler/internal/job"
	"github.com/sham-33/job-scheduler/internal/store"
)

func mustPutCron(t *testing.T, q *Queue, s CronSpec) *Cron {
	t.Helper()
	c, err := q.PutCron(context.Background(), s)
	if err != nil {
		t.Fatalf("PutCron(%s): %v", s.Name, err)
	}
	return c
}

func mustGetCron(t *testing.T, q *Queue, name string) *Cron {
	t.Helper()
	c, err := q.GetCron(context.Background(), name)
	if err != nil {
		t.Fatalf("GetCron(%s): %v", name, err)
	}
	return c
}

func mustRedisNow(t *testing.T, q *Queue) time.Time {
	t.Helper()
	now, err := q.redisNow(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return now
}

// currentMinute returns the start of the minute on Redis's clock. Near the
// end of a minute it waits for the next one first, so a test that fires a
// minutely cron's slot doesn't see the following slot come due as well.
func currentMinute(t *testing.T, q *Queue) time.Time {
	t.Helper()
	now := mustRedisNow(t, q)
	if left := now.Truncate(time.Minute).Add(time.Minute).Sub(now); left < 2*time.Second {
		time.Sleep(left + 10*time.Millisecond)
		now = mustRedisNow(t, q)
	}
	return now.Truncate(time.Minute)
}

// makeDue sets a cron's next slot, as though time had moved on to it.
func makeDue(t *testing.T, rdb *redis.Client, keys store.Keys, name string, slot time.Time) {
	t.Helper()
	ctx := context.Background()
	ms := slot.UnixMilli()
	if err := rdb.HSet(ctx, keys.Cron(name), "next_run", ms).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.ZAdd(ctx, keys.Crons(), redis.Z{Score: float64(ms), Member: name}).Err(); err != nil {
		t.Fatal(err)
	}
}

func mustFire(t *testing.T, q *Queue) []CronFire {
	t.Helper()
	fires, err := q.FireDueCrons(context.Background(), 100)
	if err != nil {
		t.Fatalf("FireDueCrons: %v", err)
	}
	return fires
}

func cronScore(t *testing.T, rdb *redis.Client, keys store.Keys, name string) float64 {
	t.Helper()
	score, ok := inZSet(t, rdb, keys.Crons(), name)
	if !ok {
		t.Fatalf("cron %s is not in the crons set", name)
	}
	return score
}

func TestPutCronValidates(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	echo := Spec{Type: "echo"}
	tests := map[string]CronSpec{
		"empty name":           {Name: "", Schedule: "@hourly", Job: echo},
		"name with a space":    {Name: "a b", Schedule: "@hourly", Job: echo},
		"name with a colon":    {Name: "a:b", Schedule: "@hourly", Job: echo},
		"name starting with -": {Name: "-a", Schedule: "@hourly", Job: echo},
		"name too long":        {Name: strings.Repeat("a", 101), Schedule: "@hourly", Job: echo},
		"bad schedule":         {Name: "a", Schedule: "every day", Job: echo},
		"never fires":          {Name: "a", Schedule: "0 0 30 2 *", Job: echo},
		"unknown zone":         {Name: "a", Schedule: "CRON_TZ=Mars/Olympus 0 9 * * *", Job: echo},
		"no job type":          {Name: "a", Schedule: "@hourly"},
		"invalid payload":      {Name: "a", Schedule: "@hourly", Job: Spec{Type: "echo", Payload: json.RawMessage(`{`)}},
		"delay":                {Name: "a", Schedule: "@hourly", Job: Spec{Type: "echo", Delay: time.Second}},
		"idempotency key":      {Name: "a", Schedule: "@hourly", Job: Spec{Type: "echo", IdempotencyKey: "k"}},
		"bad retry settings":   {Name: "a", Schedule: "@hourly", Job: Spec{Type: "echo", MaxAttempts: -1}},
	}
	for name, s := range tests {
		if _, err := q.PutCron(context.Background(), s); err == nil {
			t.Errorf("%s: PutCron succeeded, want an error", name)
		}
	}
	if n := rdb.ZCard(context.Background(), keys.Crons()).Val(); n != 0 {
		t.Errorf("%d crons saved, want none", n)
	}
}

func TestEnqueueRejectsCronIdempotencyKeys(t *testing.T) {
	q, _, _ := newTestQueue(t)
	if _, _, err := q.Enqueue(context.Background(), Spec{Type: "echo", IdempotencyKey: "cron:nightly:1"}); err == nil {
		t.Error("Enqueue with a cron: idempotency key succeeded, want an error")
	}
}

func TestPutCron(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	before := mustRedisNow(t, q)

	c := mustPutCron(t, q, CronSpec{
		Name:     "minutely",
		Schedule: "*/1 * * * *",
		Job: Spec{Type: "echo", Payload: json.RawMessage(`{"msg":"tick"}`), MaxAttempts: 3,
			Timeout: 2 * time.Second, Backoff: Backoff{Base: 100 * time.Millisecond, Max: time.Second}},
	})

	if c.Name != "minutely" || c.Schedule != "*/1 * * * *" || !c.Enabled {
		t.Errorf("cron = %+v, want enabled minutely on */1 * * * *", c)
	}
	if c.Job.Type != "echo" || string(c.Job.Payload) != `{"msg":"tick"}` || c.Job.MaxAttempts != 3 ||
		c.Job.Timeout != 2*time.Second || c.Job.Backoff != (Backoff{100 * time.Millisecond, time.Second}) {
		t.Errorf("job template = %+v", c.Job)
	}
	// The first slot is the next whole minute.
	if !c.NextRun.After(before) || c.NextRun.Sub(before) > time.Minute || c.NextRun.Second() != 0 || c.NextRun.Nanosecond() != 0 {
		t.Errorf("NextRun = %s, want the first whole minute after %s", c.NextRun, before)
	}
	if got := cronScore(t, rdb, keys, "minutely"); int64(got) != c.NextRun.UnixMilli() {
		t.Errorf("crons score = %v, want NextRun %d", got, c.NextRun.UnixMilli())
	}
	if c.CreatedAt.IsZero() || !c.UpdatedAt.Equal(c.CreatedAt) || !c.LastFired.IsZero() || c.LastJobID != "" {
		t.Errorf("cron = %+v, want a creation time and no fires yet", c)
	}

	// Defaults are filled in, as for a job submitted directly.
	d := mustPutCron(t, q, CronSpec{Name: "defaults", Schedule: "@hourly", Job: Spec{Type: "echo"}})
	if d.Job.MaxAttempts != DefaultMaxAttempts || d.Job.Backoff != (Backoff{DefaultBackoffBase, DefaultBackoffMax}) {
		t.Errorf("default template = %+v", d.Job)
	}

	list, err := q.ListCrons(context.Background())
	if err != nil || len(list) != 2 || list[0].Name == list[1].Name {
		t.Errorf("ListCrons = %v, %v; want minutely and defaults", list, err)
	}
}

func TestFireDueCron(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	mustPutCron(t, q, CronSpec{
		Name:     "minutely",
		Schedule: "*/1 * * * *",
		Job:      Spec{Type: "echo", Payload: json.RawMessage(`{"n":1}`), MaxAttempts: 2, Timeout: time.Second},
	})
	slot := currentMinute(t, q)
	makeDue(t, rdb, keys, "minutely", slot)

	fires := mustFire(t, q)
	if len(fires) != 1 {
		t.Fatalf("fired %d times, want once: %+v", len(fires), fires)
	}
	f := fires[0]
	if f.Name != "minutely" || !f.Slot.Equal(slot) || !f.Created || f.Skipped || !f.Next.Equal(slot.Add(time.Minute)) {
		t.Errorf("fire = %+v, want a new job for %s and next slot %s", f, slot, slot.Add(time.Minute))
	}

	j := mustGet(t, q, f.JobID)
	wantKey := "cron:minutely:" + strconv.FormatInt(slot.UnixMilli(), 10)
	if j.Status != job.StatusReady || j.Type != "echo" || string(j.Payload) != `{"n":1}` ||
		j.MaxAttempts != 2 || j.Timeout != time.Second || j.IdempotencyKey != wantKey {
		t.Errorf("job = %+v, want a ready echo job from the template with key %s", j, wantKey)
	}
	// run_at is the slot, so the job queues by when it was due.
	if !j.RunAt.Equal(slot) {
		t.Errorf("RunAt = %s, want the slot %s", j.RunAt, slot)
	}
	if score, ok := inZSet(t, rdb, keys.Ready(), f.JobID); !ok || int64(score) != slot.UnixMilli() {
		t.Errorf("ready score = %v (present %v), want %d", score, ok, slot.UnixMilli())
	}
	if got, _ := rdb.Get(context.Background(), keys.Idempotency(wantKey)).Result(); got != f.JobID {
		t.Errorf("idempotency key maps to %q, want %s", got, f.JobID)
	}

	c := mustGetCron(t, q, "minutely")
	if !c.NextRun.Equal(slot.Add(time.Minute)) || !c.LastFired.Equal(slot) || c.LastJobID != f.JobID {
		t.Errorf("cron = %+v, want next %s, last fired %s with job %s", c, slot.Add(time.Minute), slot, f.JobID)
	}
	if got := cronScore(t, rdb, keys, "minutely"); int64(got) != slot.Add(time.Minute).UnixMilli() {
		t.Errorf("crons score = %v, want the next slot", got)
	}
	if fired, submitted := stat(t, rdb, keys, "cron_fired"), stat(t, rdb, keys, "submitted"); fired != 1 || submitted != 1 {
		t.Errorf("stats cron_fired = %d, submitted = %d; want 1 and 1", fired, submitted)
	}

	if again := mustFire(t, q); len(again) != 0 {
		t.Errorf("fired again before the next slot: %+v", again)
	}
}

func TestFireDueCronSkipsMissedSlots(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	mustPutCron(t, q, CronSpec{Name: "minutely", Schedule: "*/1 * * * *", Job: Spec{Type: "echo"}})
	// No scheduler has run for five minutes.
	slot := mustRedisNow(t, q).Truncate(time.Minute).Add(-5 * time.Minute)
	makeDue(t, rdb, keys, "minutely", slot)

	fires := mustFire(t, q)
	if len(fires) != 1 {
		t.Fatalf("fired %d times, want once: %+v", len(fires), fires)
	}
	f := fires[0]
	if !f.Slot.Equal(slot) || !f.Skipped || f.Late < 5*time.Minute {
		t.Errorf("fire = %+v, want the oldest missed slot %s, late by 5m or more, with the rest skipped", f, slot)
	}
	// Back on schedule: the first whole minute after it fired, not the next
	// of the missed ones.
	firedAt := f.Slot.Add(f.Late)
	if want := firedAt.Truncate(time.Minute).Add(time.Minute); !f.Next.Equal(want) {
		t.Errorf("Next = %s, want %s, the first whole minute after it fired at %s", f.Next, want, firedAt)
	}
	if again := mustFire(t, q); len(again) != 0 {
		t.Errorf("fired for a missed slot again: %+v", again)
	}
	if got := stat(t, rdb, keys, "submitted"); got != 1 {
		t.Errorf("stats.submitted = %d, want 1", got)
	}
}

func TestConcurrentFiresMakeOneJobPerSlot(t *testing.T) {
	const (
		crons      = 5
		schedulers = 20
	)
	q, rdb, keys := newTestQueue(t)
	slot := mustRedisNow(t, q).Truncate(time.Minute)
	for i := range crons {
		name := "c" + strconv.Itoa(i)
		mustPutCron(t, q, CronSpec{Name: name, Schedule: "*/1 * * * *", Job: Spec{Type: "echo"}})
		makeDue(t, rdb, keys, name, slot)
	}

	// Many schedulers that all believe they lead fire the same due crons at once.
	var (
		mu      sync.Mutex
		created = map[string]int{}
		wg      sync.WaitGroup
		start   = make(chan struct{})
	)
	for range schedulers {
		wg.Go(func() {
			<-start
			fires, err := q.FireDueCrons(context.Background(), 100)
			if err != nil {
				t.Errorf("FireDueCrons: %v", err)
			}
			mu.Lock()
			defer mu.Unlock()
			for _, f := range fires {
				if f.Created {
					created[f.Name]++
				}
			}
		})
	}
	close(start)
	wg.Wait()

	for i := range crons {
		name := "c" + strconv.Itoa(i)
		if created[name] != 1 {
			t.Errorf("cron %s created %d jobs, want 1", name, created[name])
		}
	}
	if fired, submitted := stat(t, rdb, keys, "cron_fired"), stat(t, rdb, keys, "submitted"); fired != crons || submitted != crons {
		t.Errorf("stats cron_fired = %d, submitted = %d; want %d each", fired, submitted, crons)
	}
}

func TestStaleFireIsRejected(t *testing.T) {
	ctx := context.Background()
	q, rdb, keys := newTestQueue(t)
	mustPutCron(t, q, CronSpec{Name: "minutely", Schedule: "*/1 * * * *", Job: Spec{Type: "echo"}})
	slot := mustRedisNow(t, q).Truncate(time.Minute)

	// readDue plays an old leader that reads the due cron, then pauses.
	readDue := func(t *testing.T) (time.Time, dueCron) {
		t.Helper()
		makeDue(t, rdb, keys, "minutely", slot)
		now, due, err := q.dueCrons(ctx, 10)
		if err != nil || len(due) != 1 {
			t.Fatalf("dueCrons = %v, %v; want the one cron", due, err)
		}
		return now, due[0]
	}
	tests := map[string]func(t *testing.T){
		// The new leader fires the slot while the old one is paused.
		"slot fired by another scheduler": func(t *testing.T) { mustFire(t, q) },
		// Saving the cron restarts its schedule.
		"cron saved again": func(t *testing.T) {
			mustPutCron(t, q, CronSpec{Name: "minutely", Schedule: "*/2 * * * *", Job: Spec{Type: "echo"}})
		},
		// Saved again by a process that read the clock just before the slot,
		// so the new schedule's first slot is the very one the old leader
		// read. Firing it would work out the next slot from the old schedule.
		"cron saved again onto the same slot": func(t *testing.T) {
			mustPutCron(t, q, CronSpec{Name: "minutely", Schedule: "0 * * * *", Job: Spec{Type: "echo"}})
			makeDue(t, rdb, keys, "minutely", slot)
		},
		"cron disabled": func(t *testing.T) {
			if _, err := q.SetCronEnabled(ctx, "minutely", false); err != nil {
				t.Fatal(err)
			}
		},
		"cron deleted": func(t *testing.T) {
			if err := q.DeleteCron(ctx, "minutely"); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, meanwhile := range tests {
		t.Run(name, func(t *testing.T) {
			mustPutCron(t, q, CronSpec{Name: "minutely", Schedule: "*/1 * * * *", Job: Spec{Type: "echo"}})
			if _, err := q.SetCronEnabled(ctx, "minutely", true); err != nil {
				t.Fatal(err)
			}
			submitted := stat(t, rdb, keys, "submitted")
			now, d := readDue(t)
			meanwhile(t)
			made := stat(t, rdb, keys, "submitted") - submitted

			_, fired, err := q.fireCron(ctx, d, now)
			if err != nil || fired {
				t.Errorf("stale fire: fired = %v, err = %v; want rejected", fired, err)
			}
			if got := stat(t, rdb, keys, "submitted") - submitted; got != made {
				t.Errorf("the stale fire made a job: %d submitted, want %d", got, made)
			}
		})
	}
}

func TestFireRefusesSlotNotYetDue(t *testing.T) {
	ctx := context.Background()
	q, rdb, keys := newTestQueue(t)
	c := mustPutCron(t, q, CronSpec{Name: "minutely", Schedule: "*/1 * * * *", Job: Spec{Type: "echo"}})
	// A caller whose clock runs ahead of Redis's.
	d := dueCron{name: "minutely", schedule: "*/1 * * * *", slot: strconv.FormatInt(c.NextRun.UnixMilli(), 10)}
	if _, fired, err := q.fireCron(ctx, d, c.NextRun); err != nil || fired {
		t.Errorf("fireCron before the slot: fired = %v, err = %v; want refused", fired, err)
	}
	if got := stat(t, rdb, keys, "submitted"); got != 0 {
		t.Errorf("stats.submitted = %d, want 0", got)
	}
}

func TestFiredSlotNeverGetsASecondJob(t *testing.T) {
	q, rdb, keys := newTestQueue(t)
	mustPutCron(t, q, CronSpec{Name: "minutely", Schedule: "*/1 * * * *", Job: Spec{Type: "echo"}})
	slot := currentMinute(t, q)
	makeDue(t, rdb, keys, "minutely", slot)
	first := mustFire(t, q)[0]

	// The cron comes back round to a slot that already fired, as it could if
	// it were deleted and saved again by a process whose clock lagged.
	makeDue(t, rdb, keys, "minutely", slot)
	fires := mustFire(t, q)
	if len(fires) != 1 || fires[0].Created || fires[0].JobID != first.JobID {
		t.Fatalf("fires = %+v, want the first job %s and no new one", fires, first.JobID)
	}
	if got := stat(t, rdb, keys, "submitted"); got != 1 {
		t.Errorf("stats.submitted = %d, want 1", got)
	}
	// It still moved on.
	if c := mustGetCron(t, q, "minutely"); !c.NextRun.Equal(slot.Add(time.Minute)) {
		t.Errorf("NextRun = %s, want %s", c.NextRun, slot.Add(time.Minute))
	}
}

func TestDisabledCronNeverFires(t *testing.T) {
	ctx := context.Background()
	q, rdb, keys := newTestQueue(t)
	mustPutCron(t, q, CronSpec{Name: "minutely", Schedule: "*/1 * * * *", Job: Spec{Type: "echo"}})
	makeDue(t, rdb, keys, "minutely", mustRedisNow(t, q).Add(-time.Hour))

	c, err := q.SetCronEnabled(ctx, "minutely", false)
	if err != nil {
		t.Fatalf("disable: %v", err)
	}
	if c.Enabled || !c.NextRun.IsZero() {
		t.Errorf("disabled cron = %+v, want no next run", c)
	}
	if got := cronScore(t, rdb, keys, "minutely"); !math.IsInf(got, 1) {
		t.Errorf("crons score = %v, want +inf", got)
	}
	if fires := mustFire(t, q); len(fires) != 0 {
		t.Errorf("disabled cron fired: %+v", fires)
	}
	if _, err := q.SetCronEnabled(ctx, "minutely", false); err != nil {
		t.Errorf("disabling twice: %v", err)
	}

	// Turned back on, it starts again from now: the hour it was off and the
	// slot that was due never fire.
	now := mustRedisNow(t, q)
	c, err = q.SetCronEnabled(ctx, "minutely", true)
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	if !c.Enabled || !c.NextRun.After(now) || c.NextRun.Sub(now) > time.Minute {
		t.Errorf("enabled cron = %+v, want its next slot within a minute of %s", c, now)
	}
	if fires := mustFire(t, q); len(fires) != 0 {
		t.Errorf("re-enabled cron fired for a slot that passed while it was off: %+v", fires)
	}
	// Enabling an enabled cron leaves its schedule alone, so it can't push
	// back a slot that is due and about to fire.
	due := mustRedisNow(t, q).Truncate(time.Minute)
	makeDue(t, rdb, keys, "minutely", due)
	again, err := q.SetCronEnabled(ctx, "minutely", true)
	if err != nil || !again.NextRun.Equal(due) {
		t.Errorf("enabling twice: %+v, %v; want NextRun still the due slot %s", again, err, due)
	}

	for _, enabled := range []bool{true, false} {
		if _, err := q.SetCronEnabled(ctx, "nope", enabled); !errors.Is(err, ErrCronNotFound) {
			t.Errorf("SetCronEnabled(nope, %v) = %v, want ErrCronNotFound", enabled, err)
		}
	}
}

func TestEnableRejectsScheduleChangedMeanwhile(t *testing.T) {
	q, _, _ := newTestQueue(t)
	mustPutCron(t, q, CronSpec{Name: "c", Schedule: "@hourly", Job: Spec{Type: "echo"}})
	if _, err := q.SetCronEnabled(context.Background(), "c", false); err != nil {
		t.Fatal(err)
	}
	// The next slot was worked out from @hourly, but the cron is now daily.
	err := q.setCronEnabled(context.Background(), "c", true, time.Now().UnixMilli(), "@daily")
	if !errors.Is(err, ErrCronChanged) {
		t.Errorf("setCronEnabled = %v, want ErrCronChanged", err)
	}
	if c := mustGetCron(t, q, "c"); c.Enabled {
		t.Error("cron was enabled with a slot from the wrong schedule")
	}
}

func TestPutCronReplaces(t *testing.T) {
	ctx := context.Background()
	q, rdb, keys := newTestQueue(t)
	orig := mustPutCron(t, q, CronSpec{Name: "report", Schedule: "*/1 * * * *", Job: Spec{Type: "echo"}})
	makeDue(t, rdb, keys, "report", mustRedisNow(t, q).Truncate(time.Minute))
	fired := mustFire(t, q)[0]
	if _, err := q.SetCronEnabled(ctx, "report", false); err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Millisecond)
	c := mustPutCron(t, q, CronSpec{Name: "report", Schedule: "@daily",
		Job: Spec{Type: "http", Payload: json.RawMessage(`{"url":"http://x"}`)}})
	if c.Enabled || !c.NextRun.IsZero() {
		t.Errorf("replaced cron = %+v, want it still disabled", c)
	}
	if c.Schedule != "@daily" || c.Job.Type != "http" || string(c.Job.Payload) != `{"url":"http://x"}` {
		t.Errorf("replaced cron = %+v, want the new definition", c)
	}
	if !c.CreatedAt.Equal(orig.CreatedAt) || !c.UpdatedAt.After(orig.UpdatedAt) {
		t.Errorf("created %s, updated %s; want created kept at %s and updated later", c.CreatedAt, c.UpdatedAt, orig.CreatedAt)
	}
	if !c.LastFired.Equal(fired.Slot) || c.LastJobID != fired.JobID {
		t.Errorf("replaced cron = %+v, want its last fire kept", c)
	}

	// Enabled now, it runs on the new schedule: the next midnight UTC.
	c, err := q.SetCronEnabled(ctx, "report", true)
	if err != nil {
		t.Fatal(err)
	}
	if !c.NextRun.Equal(mustRedisNow(t, q).UTC().Truncate(24 * time.Hour).Add(24 * time.Hour)) {
		t.Errorf("NextRun = %s, want the next midnight UTC", c.NextRun.UTC())
	}

	// A cron that is enabled stays enabled when saved again.
	mustPutCron(t, q, CronSpec{Name: "on", Schedule: "@hourly", Job: Spec{Type: "echo"}})
	if c := mustPutCron(t, q, CronSpec{Name: "on", Schedule: "@daily", Job: Spec{Type: "echo"}}); !c.Enabled || c.NextRun.IsZero() {
		t.Errorf("re-saved cron = %+v, want enabled", c)
	}
}

func TestDeleteCron(t *testing.T) {
	ctx := context.Background()
	q, rdb, keys := newTestQueue(t)
	mustPutCron(t, q, CronSpec{Name: "minutely", Schedule: "*/1 * * * *", Job: Spec{Type: "echo"}})
	makeDue(t, rdb, keys, "minutely", mustRedisNow(t, q).Truncate(time.Minute))
	f := mustFire(t, q)[0]

	if err := q.DeleteCron(ctx, "minutely"); err != nil {
		t.Fatalf("DeleteCron: %v", err)
	}
	if _, err := q.GetCron(ctx, "minutely"); !errors.Is(err, ErrCronNotFound) {
		t.Errorf("GetCron after delete = %v, want ErrCronNotFound", err)
	}
	if _, ok := inZSet(t, rdb, keys.Crons(), "minutely"); ok {
		t.Error("deleted cron is still in the crons set")
	}
	if err := q.DeleteCron(ctx, "minutely"); !errors.Is(err, ErrCronNotFound) {
		t.Errorf("deleting twice = %v, want ErrCronNotFound", err)
	}
	// The job it made is untouched.
	if j := mustGet(t, q, f.JobID); j.Status != job.StatusReady {
		t.Errorf("job of deleted cron is %s, want ready", j.Status)
	}
	if list, err := q.ListCrons(ctx); err != nil || len(list) != 0 {
		t.Errorf("ListCrons = %v, %v; want none", list, err)
	}
}

func TestFireDueCronsDisablesUnreadableSchedule(t *testing.T) {
	ctx := context.Background()
	q, rdb, keys := newTestQueue(t)
	slot := mustRedisNow(t, q).Truncate(time.Minute)
	for _, name := range []string{"good", "broken"} {
		mustPutCron(t, q, CronSpec{Name: name, Schedule: "*/1 * * * *", Job: Spec{Type: "echo"}})
		makeDue(t, rdb, keys, name, slot)
	}
	// Edited in Redis by hand.
	rdb.HSet(ctx, keys.Cron("broken"), "schedule", "whenever")

	fires, err := q.FireDueCrons(ctx, 100)
	if err == nil || !strings.Contains(err.Error(), "broken") {
		t.Errorf("FireDueCrons error = %v, want one naming the broken cron", err)
	}
	if len(fires) != 1 || fires[0].Name != "good" {
		t.Errorf("fires = %+v, want the good cron to fire regardless", fires)
	}
	if c := mustGetCron(t, q, "broken"); c.Enabled {
		t.Error("broken cron is still enabled")
	}
	// Disabled, it isn't reported again on every call.
	if _, err := q.FireDueCrons(ctx, 100); err != nil {
		t.Errorf("second FireDueCrons: %v", err)
	}
}

func TestListCronsOrder(t *testing.T) {
	ctx := context.Background()
	q, _, _ := newTestQueue(t)
	for _, s := range []CronSpec{
		{Name: "slow", Schedule: "@every 1h", Job: Spec{Type: "echo"}},
		{Name: "off", Schedule: "@every 1s", Job: Spec{Type: "echo"}},
		{Name: "often", Schedule: "@every 10s", Job: Spec{Type: "echo"}},
	} {
		mustPutCron(t, q, s)
	}
	if _, err := q.SetCronEnabled(ctx, "off", false); err != nil {
		t.Fatal(err)
	}
	list, err := q.ListCrons(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, c := range list {
		names = append(names, c.Name)
	}
	if strings.Join(names, ",") != "often,slow,off" {
		t.Errorf("ListCrons order = %v, want soonest first and disabled last: often, slow, off", names)
	}
}
