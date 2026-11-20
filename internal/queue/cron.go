package queue

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// A cron is a job template plus a schedule. The leading scheduler fires it
// at each scheduled time, called a slot, and every slot gets exactly one
// job: the job's idempotency key is cron:{name}:{slot in ms}, and firing is
// one Lua script that also moves the cron on to its next slot.

var (
	ErrCronNotFound = errors.New("cron not found")
	// ErrCronChanged means the cron's schedule changed while it was being
	// enabled. Trying again uses the new schedule.
	ErrCronChanged = errors.New("cron changed while it was being enabled; try again")
)

var (
	//go:embed scripts/cron_put.lua
	cronPutSrc string
	//go:embed scripts/cron_enable.lua
	cronEnableSrc string
	//go:embed scripts/cron_delete.lua
	cronDeleteSrc string
	//go:embed scripts/cron_due.lua
	cronDueSrc string
	//go:embed scripts/cron_fire.lua
	cronFireSrc string

	cronPutScript    = newScript(cronPutSrc)
	cronEnableScript = newScript(cronEnableSrc)
	cronDeleteScript = newScript(cronDeleteSrc)
	cronDueScript    = newScript(cronDueSrc)
	cronFireScript   = newScript(cronFireSrc)
)

// cronIdemPrefix starts the idempotency key of every job a cron creates.
// Jobs submitted directly may not use it.
const cronIdemPrefix = "cron:"

// cronNamePattern keeps names safe to use in Redis keys and URL paths.
var cronNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$`)

// CronSpec describes a cron to save.
type CronSpec struct {
	// Name identifies the cron. Saving a cron under a name that is already
	// used replaces that cron.
	Name string
	// Schedule is a standard five-field cron expression ("*/5 * * * *") or a
	// descriptor such as "@hourly" or "@every 30s". It is read in UTC unless
	// it starts with CRON_TZ=<zone>, as in "CRON_TZ=Asia/Kolkata 0 9 * * *".
	Schedule string
	// Job is the template for the jobs the cron creates. Its Delay and
	// IdempotencyKey must be empty: the schedule says when each job runs, and
	// each slot gets its own key.
	Job Spec
}

// Cron is a saved cron and where its schedule has got to.
type Cron struct {
	Name     string
	Schedule string
	Job      Spec
	Enabled  bool
	// NextRun is the slot the cron fires for next. It is zero while the
	// cron is disabled.
	NextRun time.Time
	// LastFired is the latest slot the cron fired for, and LastJobID the
	// job that slot got.
	LastFired time.Time
	LastJobID string
	CreatedAt time.Time
	// UpdatedAt is when the cron was last saved, enabled or disabled.
	UpdatedAt time.Time
}

// CronFire reports one slot that fired.
type CronFire struct {
	Name string
	// Slot is the scheduled time that came due. It is also the job's run_at.
	Slot time.Time
	// JobID is the slot's job. Created is false if the slot already had one,
	// so no new job was made.
	JobID   string
	Created bool
	// Late is how long after Slot it fired.
	Late time.Duration
	// Skipped is true if later slots had passed too. They don't fire: a
	// cron that fell behind fires once and moves on to its first slot after
	// now.
	Skipped bool
	// Next is the slot the cron fires for next.
	Next time.Time
}

// PutCron saves a cron, replacing any cron with the same name, and returns
// it. Its schedule starts from now: the first slot is the first one after
// now. A replaced cron stays disabled if it was.
func (q *Queue) PutCron(ctx context.Context, s CronSpec) (*Cron, error) {
	if !cronNamePattern.MatchString(s.Name) {
		return nil, fmt.Errorf("cron name %q must be 1 to 100 letters, digits, '.', '_' or '-', starting with a letter or digit", s.Name)
	}
	sched, err := parseSchedule(s.Schedule)
	if err != nil {
		return nil, err
	}
	if s.Job.Delay != 0 || s.Job.IdempotencyKey != "" {
		return nil, errors.New("a cron's job can't have a delay or an idempotency key: the schedule sets when it runs, and each slot gets its own key")
	}
	tmpl := s.Job.withDefaults()
	if err := tmpl.validate(); err != nil {
		return nil, err
	}
	now, err := q.redisNow(ctx)
	if err != nil {
		return nil, err
	}
	next, err := firstFire(sched, s.Schedule, now)
	if err != nil {
		return nil, err
	}

	keys := []string{q.keys.Cron(s.Name), q.keys.Crons()}
	err = cronPutScript.Run(ctx, q.rdb, keys,
		s.Name, s.Schedule, next.UnixMilli(), tmpl.Type, string(tmpl.Payload), tmpl.MaxAttempts,
		tmpl.Timeout.Milliseconds(), tmpl.Backoff.Base.Milliseconds(), tmpl.Backoff.Max.Milliseconds(),
	).Err()
	if err != nil {
		return nil, fmt.Errorf("put cron: %w", err)
	}
	return q.GetCron(ctx, s.Name)
}

// GetCron returns the cron with the given name, or ErrCronNotFound.
func (q *Queue) GetCron(ctx context.Context, name string) (*Cron, error) {
	h, err := q.rdb.HGetAll(ctx, q.keys.Cron(name)).Result()
	if err != nil {
		return nil, fmt.Errorf("get cron %s: %w", name, err)
	}
	if len(h) == 0 {
		return nil, ErrCronNotFound
	}
	return cronFromHash(h)
}

// ListCrons returns every cron, the one that fires soonest first and
// disabled ones last.
func (q *Queue) ListCrons(ctx context.Context) ([]*Cron, error) {
	names, err := q.rdb.ZRange(ctx, q.keys.Crons(), 0, -1).Result()
	if err != nil {
		return nil, fmt.Errorf("list crons: %w", err)
	}
	crons := make([]*Cron, 0, len(names))
	if len(names) == 0 {
		return crons, nil
	}
	pipe := q.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(names))
	for i, name := range names {
		cmds[i] = pipe.HGetAll(ctx, q.keys.Cron(name))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("list crons: %w", err)
	}
	for _, cmd := range cmds {
		if len(cmd.Val()) == 0 {
			continue // deleted since the names were read
		}
		c, err := cronFromHash(cmd.Val())
		if err != nil {
			return nil, err
		}
		crons = append(crons, c)
	}
	return crons, nil
}

// DeleteCron deletes a cron, or returns ErrCronNotFound. Jobs it already
// created are left alone.
func (q *Queue) DeleteCron(ctx context.Context, name string) error {
	n, err := cronDeleteScript.Run(ctx, q.rdb, []string{q.keys.Cron(name), q.keys.Crons()}, name).Int()
	if err != nil {
		return fmt.Errorf("delete cron: %w", err)
	}
	if n == 0 {
		return ErrCronNotFound
	}
	return nil
}

// SetCronEnabled turns a cron on or off and returns it. A disabled cron
// never fires. One turned back on restarts from its first slot after now,
// so the slots it missed while off never fire.
func (q *Queue) SetCronEnabled(ctx context.Context, name string, enabled bool) (*Cron, error) {
	if !enabled {
		if err := q.setCronEnabled(ctx, name, false, 0, ""); err != nil {
			return nil, err
		}
		return q.GetCron(ctx, name)
	}
	c, err := q.GetCron(ctx, name)
	if err != nil {
		return nil, err
	}
	sched, err := parseSchedule(c.Schedule)
	if err != nil {
		return nil, err
	}
	now, err := q.redisNow(ctx)
	if err != nil {
		return nil, err
	}
	next, err := firstFire(sched, c.Schedule, now)
	if err != nil {
		return nil, err
	}
	// The script checks the schedule is still the one next came from.
	if err := q.setCronEnabled(ctx, name, true, next.UnixMilli(), c.Schedule); err != nil {
		return nil, err
	}
	return q.GetCron(ctx, name)
}

func (q *Queue) setCronEnabled(ctx context.Context, name string, enabled bool, nextMs int64, schedule string) error {
	flag := "0"
	if enabled {
		flag = "1"
	}
	code, err := cronEnableScript.Run(ctx, q.rdb, []string{q.keys.Cron(name), q.keys.Crons()},
		name, flag, nextMs, schedule).Int()
	switch {
	case err != nil:
		return fmt.Errorf("set cron enabled: %w", err)
	case code == -1:
		return ErrCronNotFound
	case code == 0:
		return ErrCronChanged
	}
	return nil
}

// FireDueCrons fires up to limit crons whose next slot has come, each for
// one slot, and reports what fired. It is safe to call from several
// schedulers at once: each slot still fires once.
//
// A cron whose saved schedule can't be read any more (it was edited in Redis
// by hand, say) is disabled rather than retried on every call, and reported
// in the error alongside any other failures.
func (q *Queue) FireDueCrons(ctx context.Context, limit int) ([]CronFire, error) {
	now, due, err := q.dueCrons(ctx, limit)
	if err != nil {
		return nil, err
	}
	var fires []CronFire
	var errs []error
	for _, d := range due {
		f, fired, err := q.fireCron(ctx, d, now)
		if err != nil {
			errs = append(errs, err)
		} else if fired {
			fires = append(fires, f)
		}
	}
	return fires, errors.Join(errs...)
}

// dueCron is a cron whose next slot has come, as read before firing it.
type dueCron struct {
	name     string
	schedule string
	// slot is next_run exactly as stored. The fire script compares the two
	// as strings, so it is passed back untouched.
	slot string
}

// dueCrons lists up to limit due crons and returns Redis's clock at the time.
func (q *Queue) dueCrons(ctx context.Context, limit int) (time.Time, []dueCron, error) {
	reply, err := cronDueScript.Run(ctx, q.rdb, []string{q.keys.Crons()}, q.keys.CronPrefix(), limit).Slice()
	if err != nil {
		return time.Time{}, nil, fmt.Errorf("due crons: %w", err)
	}
	if len(reply)%3 != 1 {
		return time.Time{}, nil, fmt.Errorf("due crons: unexpected reply %v", reply)
	}
	nowMs, ok := reply[0].(int64)
	if !ok {
		return time.Time{}, nil, fmt.Errorf("due crons: unexpected reply %v", reply)
	}
	due := make([]dueCron, 0, len(reply)/3)
	for i := 1; i < len(reply); i += 3 {
		name, ok1 := reply[i].(string)
		schedule, ok2 := reply[i+1].(string)
		slot, ok3 := reply[i+2].(string)
		if !ok1 || !ok2 || !ok3 {
			return time.Time{}, nil, fmt.Errorf("due crons: unexpected reply %v", reply)
		}
		due = append(due, dueCron{name: name, schedule: schedule, slot: slot})
	}
	return time.UnixMilli(nowMs), due, nil
}

// fireCron fires d's slot, if no one else has yet. fired is false when the
// cron was no longer waiting for that slot.
func (q *Queue) fireCron(ctx context.Context, d dueCron, now time.Time) (f CronFire, fired bool, err error) {
	slotMs, err := strconv.ParseInt(d.slot, 10, 64)
	if err != nil {
		return CronFire{}, false, q.disableBrokenCron(ctx, d.name, fmt.Errorf("next_run %q is not a number", d.slot))
	}
	slot := time.UnixMilli(slotMs)
	sched, err := parseSchedule(d.schedule)
	if err != nil {
		return CronFire{}, false, q.disableBrokenCron(ctx, d.name, err)
	}
	next, skipped := nextFire(sched, slot, now)
	if next.IsZero() {
		return CronFire{}, false, q.disableBrokenCron(ctx, d.name, fmt.Errorf("schedule %q never fires again", d.schedule))
	}

	uid, err := uuid.NewV7()
	if err != nil {
		return CronFire{}, false, fmt.Errorf("new job id: %w", err)
	}
	id := uid.String()
	idem := cronIdemPrefix + d.name + ":" + d.slot
	keys := []string{q.keys.Cron(d.name), q.keys.Crons(), q.keys.Job(id), q.keys.Ready(), q.keys.Scheduled(),
		q.keys.Idempotency(idem), q.keys.Stats(), q.keys.Events()}
	reply, err := cronFireScript.Run(ctx, q.rdb, keys,
		d.name, d.slot, d.schedule, next.UnixMilli(), id, idem, int(IdempotencyTTL.Seconds()),
	).Slice()
	if err != nil {
		return CronFire{}, false, fmt.Errorf("fire cron %s: %w", d.name, err)
	}
	if len(reply) == 1 && reply[0] == int64(0) {
		return CronFire{}, false, nil
	}
	var code int64
	var jobID string
	if len(reply) == 2 {
		code, _ = reply[0].(int64)
		jobID, _ = reply[1].(string)
	}
	if (code != 1 && code != 2) || jobID == "" {
		return CronFire{}, false, fmt.Errorf("fire cron %s: unexpected reply %v", d.name, reply)
	}
	return CronFire{
		Name:    d.name,
		Slot:    slot,
		JobID:   jobID,
		Created: code == 1,
		Late:    now.Sub(slot),
		Skipped: skipped,
		Next:    next,
	}, true, nil
}

func (q *Queue) disableBrokenCron(ctx context.Context, name string, cause error) error {
	if err := q.setCronEnabled(ctx, name, false, 0, ""); err != nil && !errors.Is(err, ErrCronNotFound) {
		return fmt.Errorf("cron %s: %w; disabling it failed too: %w", name, cause, err)
	}
	return fmt.Errorf("cron %s: %w; disabled it", name, cause)
}

// redisNow reads Redis's clock, the one every process shares, to the
// millisecond like the scripts do.
func (q *Queue) redisNow(ctx context.Context) (time.Time, error) {
	t, err := q.rdb.Time(ctx).Result()
	if err != nil {
		return time.Time{}, fmt.Errorf("read redis clock: %w", err)
	}
	return t.Truncate(time.Millisecond), nil
}

// cronFromHash builds a Cron from the fields of its Redis hash.
func cronFromHash(h map[string]string) (*Cron, error) {
	var bad error
	num := func(field string) int64 {
		v := h[field]
		if v == "" {
			return 0
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil && bad == nil {
			bad = fmt.Errorf("field %s: %q is not an integer", field, v)
		}
		return n
	}
	ms := func(field string) time.Duration { return time.Duration(num(field)) * time.Millisecond }
	at := func(field string) time.Time {
		if n := num(field); n != 0 {
			return time.UnixMilli(n)
		}
		return time.Time{}
	}

	c := &Cron{
		Name:     h["name"],
		Schedule: h["schedule"],
		Job: Spec{
			Type:        h["type"],
			MaxAttempts: int(num("max_attempts")),
			Timeout:     ms("timeout_ms"),
			Backoff:     Backoff{Base: ms("backoff_base_ms"), Max: ms("backoff_max_ms")},
		},
		Enabled:   h["enabled"] == "1",
		NextRun:   at("next_run"),
		LastFired: at("last_fired"),
		LastJobID: h["last_job_id"],
		CreatedAt: at("created_at"),
		UpdatedAt: at("updated_at"),
	}
	if payload := h["payload"]; payload != "" {
		c.Job.Payload = json.RawMessage(payload)
	}
	if bad != nil {
		return nil, fmt.Errorf("cron %s: %w", c.Name, bad)
	}
	return c, nil
}
