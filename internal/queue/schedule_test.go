package queue

import (
	"testing"
	"time"

	"github.com/robfig/cron/v3"
)

func mustParseSchedule(t *testing.T, spec string) cron.Schedule {
	t.Helper()
	s, err := parseSchedule(spec)
	if err != nil {
		t.Fatalf("parseSchedule(%q): %v", spec, err)
	}
	return s
}

func utc(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

func TestParseSchedule(t *testing.T) {
	for _, spec := range []string{
		"*/1 * * * *",
		"0 9 * * 1-5",
		"@hourly",
		"@every 30s",
		"@every 1s",
		"@every 1m30s",
		"CRON_TZ=Asia/Kolkata 0 9 * * *",
		"CRON_TZ=Asia/Kolkata @every 2s",
	} {
		if _, err := parseSchedule(spec); err != nil {
			t.Errorf("parseSchedule(%q): %v", spec, err)
		}
	}
	for _, spec := range []string{
		"",
		"* * * *",       // too few fields
		"0 */5 * * * *", // a seconds field isn't part of the standard format
		"61 * * * *",    // minute out of range
		"@every soon",   // not a duration
		"@fortnightly",  // no such descriptor
		"CRON_TZ=Nowhere/Land * * * * *",
		// robfig would fire each of these every 1s.
		"@every 1500ms",
		"@every 100ms",
		"@every 0s",
		"@every -5s",
		"CRON_TZ=Asia/Kolkata @every 1500ms",
	} {
		if _, err := parseSchedule(spec); err == nil {
			t.Errorf("parseSchedule(%q) succeeded, want an error", spec)
		}
	}
}

func TestFirstFire(t *testing.T) {
	tests := []struct {
		spec string
		now  time.Time
		want time.Time
	}{
		{"*/1 * * * *", utc("2026-11-17T12:00:30Z"), utc("2026-11-17T12:01:00Z")},
		// Strictly after now, even when now is a slot.
		{"*/1 * * * *", utc("2026-11-17T12:00:00Z"), utc("2026-11-17T12:01:00Z")},
		// Read in UTC, whatever zone now is given in: 10:00 in India is 04:30
		// UTC, so 09:00 UTC is still to come today.
		{"0 9 * * *", utc("2026-11-17T04:30:00Z").In(time.FixedZone("IST", 5*3600+1800)), utc("2026-11-17T09:00:00Z")},
		// CRON_TZ picks the zone: 09:00 in India is 03:30 UTC, which has
		// passed today.
		{"CRON_TZ=Asia/Kolkata 0 9 * * *", utc("2026-11-17T04:00:00Z"), utc("2026-11-18T03:30:00Z")},
		// @every counts from now, to the whole second.
		{"@every 10s", utc("2026-11-17T12:00:03.5Z"), utc("2026-11-17T12:00:13Z")},
	}
	for _, tt := range tests {
		got, err := firstFire(mustParseSchedule(t, tt.spec), tt.spec, tt.now)
		if err != nil || !got.Equal(tt.want) {
			t.Errorf("firstFire(%q, %s) = %s, %v; want %s", tt.spec, tt.now, got, err, tt.want)
		}
	}

	// February 30th never comes.
	if got, err := firstFire(mustParseSchedule(t, "0 0 30 2 *"), "0 0 30 2 *", time.Now()); err == nil {
		t.Errorf("firstFire for Feb 30 = %s, want an error", got)
	}
}

func TestNextFire(t *testing.T) {
	tests := []struct {
		name        string
		spec        string
		slot, now   time.Time
		want        time.Time
		wantSkipped bool
	}{
		{"on time", "*/1 * * * *",
			utc("2026-11-17T12:00:00Z"), utc("2026-11-17T12:00:00.2Z"), utc("2026-11-17T12:01:00Z"), false},
		{"late, but before the next slot", "*/1 * * * *",
			utc("2026-11-17T12:00:00Z"), utc("2026-11-17T12:00:59Z"), utc("2026-11-17T12:01:00Z"), false},
		{"late enough that the next slot is due too", "*/1 * * * *",
			utc("2026-11-17T12:00:00Z"), utc("2026-11-17T12:01:00Z"), utc("2026-11-17T12:02:00Z"), true},
		{"down for five minutes", "*/1 * * * *",
			utc("2026-11-17T12:00:00Z"), utc("2026-11-17T12:05:10Z"), utc("2026-11-17T12:06:00Z"), true},
		// @every keeps to its grid when it fires a little late...
		{"@every on its grid", "@every 10s",
			utc("2026-11-17T12:00:00Z"), utc("2026-11-17T12:00:03Z"), utc("2026-11-17T12:00:10Z"), false},
		// ...and starts a new one from now after it fell behind.
		{"@every after an outage", "@every 10s",
			utc("2026-11-17T12:00:00Z"), utc("2026-11-17T12:00:35.5Z"), utc("2026-11-17T12:00:45Z"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, skipped := nextFire(mustParseSchedule(t, tt.spec), tt.slot, tt.now)
			if !got.Equal(tt.want) || skipped != tt.wantSkipped {
				t.Errorf("nextFire = %s, skipped %v; want %s, skipped %v", got, skipped, tt.want, tt.wantSkipped)
			}
		})
	}
}
