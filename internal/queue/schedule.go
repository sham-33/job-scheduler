package queue

import (
	"fmt"
	"time"
	// Embeds the time zone database, so a CRON_TZ schedule reads the same in
	// every process, even in a container image that has no zoneinfo files.
	_ "time/tzdata"

	"github.com/robfig/cron/v3"
)

// cronParser reads standard five-field cron expressions ("*/5 * * * *") and
// descriptors such as "@hourly" and "@every 30s".
var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

func parseSchedule(spec string) (cron.Schedule, error) {
	s, err := cronParser.Parse(spec)
	if err != nil {
		return nil, fmt.Errorf("schedule %q: %w", spec, err)
	}
	return s, nil
}

// Every function below hands the schedule a UTC time. A schedule without a
// CRON_TZ prefix is read in the zone of the time it is given, so this makes
// "0 9 * * *" mean 09:00 UTC on every machine, whatever its local zone.

// firstFire returns a schedule's first slot after now.
func firstFire(s cron.Schedule, spec string, now time.Time) (time.Time, error) {
	next := s.Next(now.UTC())
	if next.IsZero() {
		return time.Time{}, fmt.Errorf("schedule %q never fires", spec)
	}
	return next, nil
}

// nextFire returns the slot to wait for once slot has fired at now. That is
// normally the slot after it. If that one has passed too, the cron fell
// behind (no scheduler was running, say), and rather than fire once for
// every slot it missed, it skips to its first slot after now and reports
// skipped. A zero next means the schedule never fires again.
func nextFire(s cron.Schedule, slot, now time.Time) (next time.Time, skipped bool) {
	next = s.Next(slot.UTC())
	if next.After(now) {
		return next, false
	}
	return s.Next(now.UTC()), true
}
