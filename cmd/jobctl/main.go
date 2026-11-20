// Command jobctl is the command-line client for the job scheduler's API.
//
//	jobctl submit echo '{"hi":1}'        submit a job and print its ID
//	jobctl get <id>                      show a job
//	jobctl dlq list                      list dead jobs
//	jobctl cron add nightly '0 2 * * *' http '{"url":"..."}'
//	jobctl stats                         leader, workers, queues and counters
//	jobctl events -f                     follow job events as they happen
//
// It talks to the API at $JOBCTL_API (default http://localhost:8080) and
// sends $API_TOKEN as a bearer token if set.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/sham-33/job-scheduler/internal/api"
)

// cli holds what every command shares.
type cli struct {
	apiURL string
	token  string
	asJSON bool
	out    io.Writer
	in     io.Reader
}

func (c *cli) client() *api.Client { return api.NewClient(c.apiURL, c.token) }

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := newRoot(&cli{out: os.Stdout, in: os.Stdin}).ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "jobctl:", err)
		os.Exit(1)
	}
}

func newRoot(c *cli) *cobra.Command {
	root := &cobra.Command{
		Use:           "jobctl",
		Short:         "Submit and manage jobs and crons, and watch the cluster",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.SetOut(c.out)
	def := os.Getenv("JOBCTL_API")
	if def == "" {
		def = "http://localhost:8080"
	}
	root.PersistentFlags().StringVar(&c.apiURL, "api", def, "API address (env JOBCTL_API)")
	root.PersistentFlags().StringVar(&c.token, "token", os.Getenv("API_TOKEN"), "API token (env API_TOKEN)")
	root.PersistentFlags().BoolVar(&c.asJSON, "json", false, "print the API's JSON reply")
	root.AddCommand(submitCmd(c), getCmd(c), cancelCmd(c), dlqCmd(c), cronCmd(c), statsCmd(c), eventsCmd(c))
	return root
}

// jobFlags are the job settings shared by `submit` and `cron add`.
type jobFlags struct {
	maxAttempts int
	timeout     time.Duration
	backoffBase time.Duration
	backoffMax  time.Duration
}

func (f *jobFlags) register(cmd *cobra.Command) {
	cmd.Flags().IntVar(&f.maxAttempts, "max-attempts", 0, "times the job may start before it is dead (default 5)")
	cmd.Flags().DurationVar(&f.timeout, "timeout", 0, "longest one attempt may run, e.g. 30s (default none)")
	cmd.Flags().DurationVar(&f.backoffBase, "backoff-base", 0, "wait before the first retry; doubles each time (default 1s)")
	cmd.Flags().DurationVar(&f.backoffMax, "backoff-max", 0, "longest wait between retries (default 1m)")
}

func (f *jobFlags) request(c *cli, typ string, payloadArgs []string) (api.JobRequest, error) {
	req := api.JobRequest{
		Type:        typ,
		MaxAttempts: f.maxAttempts,
		Timeout:     api.Duration(f.timeout),
		Backoff:     api.Backoff{Base: api.Duration(f.backoffBase), Max: api.Duration(f.backoffMax)},
	}
	if len(payloadArgs) == 0 {
		return req, nil
	}
	payload := []byte(payloadArgs[0])
	if payloadArgs[0] == "-" {
		var err error
		if payload, err = io.ReadAll(c.in); err != nil {
			return req, fmt.Errorf("read payload: %w", err)
		}
	}
	if !json.Valid(payload) {
		return req, fmt.Errorf("payload is not valid JSON: %s", payload)
	}
	req.Payload = payload
	return req, nil
}

func submitCmd(c *cli) *cobra.Command {
	var f jobFlags
	var delay time.Duration
	var key string
	cmd := &cobra.Command{
		Use:   "submit TYPE [PAYLOAD | -]",
		Short: "Submit a job and print its ID",
		Long: "Submit a job and print its ID. PAYLOAD is JSON; - reads it from stdin.\n" +
			"With --key, submitting again with the same key returns the first job instead of making another.",
		Example: `  jobctl submit echo '{"hi":1}'
  jobctl submit http '{"url":"https://example.com/hook","body":{"n":1}}' --delay 30s --timeout 10s
  jobctl submit sleep '{"ms":500}' --key order-1234`,
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			req, err := f.request(c, args[0], args[1:])
			if err != nil {
				return err
			}
			req.Delay = api.Duration(delay)
			j, created, err := c.client().SubmitJob(cmd.Context(), req, key)
			if err != nil {
				return err
			}
			if c.asJSON {
				return printJSON(c.out, j)
			}
			if !created {
				fmt.Fprintf(cmd.ErrOrStderr(), "key %q was already used; this is the job it made\n", key)
			}
			fmt.Fprintln(c.out, j.ID)
			return nil
		},
	}
	f.register(cmd)
	cmd.Flags().DurationVar(&delay, "delay", 0, "run the job this long from now, e.g. 5m")
	cmd.Flags().StringVar(&key, "key", "", "idempotency key")
	return cmd
}

func getCmd(c *cli) *cobra.Command {
	return &cobra.Command{
		Use:   "get ID",
		Short: "Show a job",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			j, err := c.client().GetJob(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			return printJSON(c.out, j)
		},
	}
}

func cancelCmd(c *cli) *cobra.Command {
	return &cobra.Command{
		Use:   "cancel ID",
		Short: "Cancel a job that hasn't finished",
		Long:  "Cancel a job that hasn't finished. A running job stops at its worker's next heartbeat.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			j, err := c.client().CancelJob(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			return c.printJob(j)
		},
	}
}

func dlqCmd(c *cli) *cobra.Command {
	dlq := &cobra.Command{Use: "dlq", Short: "List, replay or purge dead jobs"}

	var limit, offset int
	list := &cobra.Command{
		Use:   "list",
		Short: "List dead jobs, the most recent first",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			page, err := c.client().ListDLQ(cmd.Context(), offset, limit)
			if err != nil {
				return err
			}
			if c.asJSON {
				return printJSON(c.out, page)
			}
			tw := table(c.out, "ID", "TYPE", "ATTEMPTS", "DIED", "LAST ERROR")
			for _, j := range page.Jobs {
				row(tw, j.ID, j.Type, fmt.Sprintf("%d/%d", j.Attempts, j.MaxAttempts), when(j.FinishedAt), clip(j.LastError, 60))
			}
			if err := tw.Flush(); err != nil {
				return err
			}
			fmt.Fprintf(c.out, "%d of %d dead jobs\n", len(page.Jobs), page.Total)
			return nil
		},
	}
	list.Flags().IntVar(&limit, "limit", 50, "most jobs to list (up to 1000)")
	list.Flags().IntVar(&offset, "offset", 0, "skip this many of the most recent")

	replay := &cobra.Command{
		Use:   "replay ID",
		Short: "Run a dead job again, with its attempts reset",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			j, err := c.client().ReplayJob(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			return c.printJob(j)
		},
	}

	var yes bool
	purge := &cobra.Command{
		Use:   "purge --yes",
		Short: "Delete every dead job for good",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !yes {
				return fmt.Errorf("this deletes every dead job for good; run again with --yes to confirm")
			}
			n, err := c.client().PurgeDLQ(cmd.Context())
			if err != nil {
				return err
			}
			if c.asJSON {
				return printJSON(c.out, api.PurgeResult{Purged: n})
			}
			fmt.Fprintf(c.out, "deleted %d dead jobs\n", n)
			return nil
		},
	}
	purge.Flags().BoolVar(&yes, "yes", false, "confirm deleting every dead job")

	dlq.AddCommand(list, replay, purge)
	return dlq
}

func cronCmd(c *cli) *cobra.Command {
	cron := &cobra.Command{Use: "cron", Short: "Add, list, enable, disable or remove crons"}

	var f jobFlags
	add := &cobra.Command{
		Use:   "add NAME SCHEDULE TYPE [PAYLOAD | -]",
		Short: "Save a cron, replacing one with the same name",
		Long: "Save a cron, replacing one with the same name. SCHEDULE is five cron fields, in UTC unless it\n" +
			"starts with CRON_TZ=<zone>, or a shorthand such as @hourly or @every 30s.",
		Example: `  jobctl cron add heartbeat '@every 1m' http '{"url":"https://example.com/ping"}'
  jobctl cron add report 'CRON_TZ=Asia/Kolkata 0 9 * * 1-5' echo '{"report":"daily"}'`,
		Args: cobra.RangeArgs(3, 4),
		RunE: func(cmd *cobra.Command, args []string) error {
			tmpl, err := f.request(c, args[2], args[3:])
			if err != nil {
				return err
			}
			cr, err := c.client().PutCron(cmd.Context(), api.CronRequest{Name: args[0], Schedule: args[1], Job: tmpl})
			if err != nil {
				return err
			}
			return c.printCron(cr)
		},
	}
	f.register(add)

	list := &cobra.Command{
		Use:   "list",
		Short: "List crons, the one that fires soonest first",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			crons, err := c.client().ListCrons(cmd.Context())
			if err != nil {
				return err
			}
			if c.asJSON {
				return printJSON(c.out, api.CronList{Crons: crons})
			}
			tw := table(c.out, "NAME", "SCHEDULE", "TYPE", "ENABLED", "NEXT RUN", "LAST JOB")
			for _, cr := range crons {
				row(tw, cr.Name, cr.Schedule, cr.Job.Type, fmt.Sprint(cr.Enabled), when(cr.NextRun), dash(cr.LastJobID))
			}
			return tw.Flush()
		},
	}

	get := &cobra.Command{
		Use:   "get NAME",
		Short: "Show a cron",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cr, err := c.client().GetCron(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			return printJSON(c.out, cr)
		},
	}

	toggle := func(use, short string, enabled bool) *cobra.Command {
		return &cobra.Command{
			Use:   use + " NAME",
			Short: short,
			Args:  cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				cr, err := c.client().SetCronEnabled(cmd.Context(), args[0], enabled)
				if err != nil {
					return err
				}
				return c.printCron(cr)
			},
		}
	}

	rm := &cobra.Command{
		Use:   "rm NAME",
		Short: "Delete a cron. Jobs it already made are left alone",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := c.client().DeleteCron(cmd.Context(), args[0]); err != nil {
				return err
			}
			if !c.asJSON {
				fmt.Fprintf(c.out, "deleted cron %s\n", args[0])
			}
			return nil
		},
	}

	cron.AddCommand(add, list, get,
		toggle("enable", "Turn a cron on. It restarts from its next slot after now", true),
		toggle("disable", "Turn a cron off", false),
		rm)
	return cron
}

func statsCmd(c *cli) *cobra.Command {
	return &cobra.Command{
		Use:   "stats",
		Short: "Show the leader, workers, queue sizes and counters",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := c.client().Stats(cmd.Context())
			if err != nil {
				return err
			}
			if c.asJSON {
				return printJSON(c.out, s)
			}
			fmt.Fprintf(c.out, "leader   %s (term %d)\n", dash(s.Leader), s.Epoch)
			fmt.Fprintf(c.out, "queues   scheduled %d · ready %d · running %d · dead %d\n",
				s.Queues.Scheduled, s.Queues.Ready, s.Queues.Running, s.Queues.Dead)
			names := []string{"submitted", "claimed", "succeeded", "retried", "dead", "reclaimed",
				"released", "fenced", "cancelled", "replayed", "purged", "cron_fired"}
			var parts []string
			for _, n := range names {
				if v, ok := s.Counters[n]; ok {
					parts = append(parts, fmt.Sprintf("%s %d", n, v))
				}
			}
			fmt.Fprintf(c.out, "counters %s\n\n", dash(strings.Join(parts, " · ")))
			tw := table(c.out, "WORKER", "STATE", "BUSY", "LAST SEEN")
			for _, w := range s.Workers {
				state := "alive"
				switch {
				case !w.Alive:
					state = "DEAD"
				case w.Draining:
					state = "draining"
				}
				row(tw, w.ID, state, fmt.Sprintf("%d/%d", w.Busy, w.Concurrency),
					s.At.Sub(w.LastSeen).Round(100*time.Millisecond).String()+" ago")
			}
			return tw.Flush()
		},
	}
}

func eventsCmd(c *cli) *cobra.Command {
	var limit int
	var follow bool
	cmd := &cobra.Command{
		Use:   "events",
		Short: "Show the latest job events, oldest first",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cl := c.client()
			page, err := cl.Events(cmd.Context(), "", limit)
			for {
				if err != nil {
					if cmd.Context().Err() != nil {
						return nil // Ctrl-C while following
					}
					return err
				}
				for _, e := range page.Events {
					if c.asJSON {
						if err := json.NewEncoder(c.out).Encode(e); err != nil {
							return err
						}
						continue
					}
					line := fmt.Sprintf("%s  %-9s  %s  %-6s  %s → %s  attempt %d", e.At.Local().Format("15:04:05.000"),
						e.Name, e.JobID, e.JobType, dash(e.From), dash(e.To), e.Attempt)
					if e.Worker != "" {
						line += "  " + e.Worker
					}
					if e.Error != "" {
						line += "  " + clip(e.Error, 80)
					}
					fmt.Fprintln(c.out, line)
				}
				if !follow {
					return nil
				}
				select {
				case <-cmd.Context().Done():
					return nil
				case <-time.After(time.Second):
				}
				page, err = cl.Events(cmd.Context(), page.LastID, 1000)
			}
		},
	}
	cmd.Flags().IntVarP(&limit, "limit", "n", 20, "how many of the latest events to show first (up to 1000)")
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "keep printing new events until Ctrl-C")
	return cmd
}

func (c *cli) printJob(j api.Job) error {
	if c.asJSON {
		return printJSON(c.out, j)
	}
	_, err := fmt.Fprintf(c.out, "%s  %s\n", j.ID, j.Status)
	return err
}

func (c *cli) printCron(cr api.Cron) error {
	if c.asJSON {
		return printJSON(c.out, cr)
	}
	state := "disabled"
	if cr.Enabled {
		state = "next run " + when(cr.NextRun)
	}
	_, err := fmt.Fprintf(c.out, "cron %s (%s): %s\n", cr.Name, cr.Schedule, state)
	return err
}

func printJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func table(w io.Writer, headers ...string) *tabwriter.Writer {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	row(tw, headers...)
	return tw
}

func row(tw *tabwriter.Writer, cols ...string) {
	fmt.Fprintln(tw, strings.Join(cols, "\t"))
}

// when formats a time in the local zone, or "-" for none.
func when(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.Local().Format("2006-01-02 15:04:05")
}

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// clip shortens s to n characters, on one line.
func clip(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if r := []rune(s); len(r) > n {
		return string(r[:n-1]) + "…"
	}
	return s
}
