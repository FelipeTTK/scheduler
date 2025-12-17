package scheduler

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
)

var nowFunc = time.Now

// CommandRunner abstracts exec so callers/tests can override.
type CommandRunner interface {
	Run(ctx context.Context, exe string, args []string) error
}

// SchedulerAdapter wraps gocron.Scheduler with a small interface surface.
type SchedulerAdapter interface {
	NewJob(job gocron.JobDefinition, task gocron.Task, options ...gocron.JobOption) (gocron.Job, error)
	Start()
	Shutdown() error
	Jobs() []gocron.Job
}

type gocronSchedulerAdapter struct {
	s gocron.Scheduler
}

func (a gocronSchedulerAdapter) NewJob(job gocron.JobDefinition, task gocron.Task, options ...gocron.JobOption) (gocron.Job, error) {
	return a.s.NewJob(job, task, options...)
}

func (a gocronSchedulerAdapter) Start() { a.s.Start() }

func (a gocronSchedulerAdapter) Shutdown() error { return a.s.Shutdown() }

func (a gocronSchedulerAdapter) Jobs() []gocron.Job { return a.s.Jobs() }

type execCommandRunner struct{}

func (execCommandRunner) Run(ctx context.Context, exe string, args []string) error {
	cmd := exec.CommandContext(ctx, exe, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// JobBuilder is a wrapper around gocron.Job that provides a fluent interface for scheduling jobs.
type JobBuilder struct {
	scheduler         SchedulerAdapter
	timezone          string
	name              string
	err               error
	duration          *time.Duration
	cronExpr          string
	withoutOverlap    bool
	retainState       bool
	lastJob           gocron.Job
	distributedLocker gocron.Locker
	envs              []string
	whenFunc          func() bool
	skipFunc          func() bool
	extraTags         []string
	jobMetadata       map[uuid.UUID]jobMetadata
	targetKind        jobTargetKind
	commandArgs       []string
	commandRunner     CommandRunner
	now               func() time.Time
	hooks             taskHooks
	runInBackground   bool
}

type taskHooks struct {
	Before    func()
	After     func()
	OnSuccess func()
	OnFailure func()
}

// NewJobBuilder creates a new JobBuilder with the provided scheduler.
func NewJobBuilder(s gocron.Scheduler) *JobBuilder {
	return &JobBuilder{
		scheduler:     gocronSchedulerAdapter{s: s},
		jobMetadata:   make(map[uuid.UUID]jobMetadata),
		targetKind:    jobTargetFunction,
		commandRunner: execCommandRunner{},
		now:           nowFunc,
	}
}

type jobScheduleKind string

const (
	jobScheduleCron     jobScheduleKind = "cron"
	jobScheduleInterval jobScheduleKind = "interval"
	jobScheduleUnknown  jobScheduleKind = "unknown"
)

type jobTargetKind string

const (
	jobTargetFunction jobTargetKind = "function"
	jobTargetCommand  jobTargetKind = "command"
)

type jobMetadata struct {
	id           uuid.UUID
	name         string
	schedule     string
	scheduleType jobScheduleKind
	targetKind   jobTargetKind
	handler      string
	command      string
	tags         []string
}

// RetainState allows the job to retain its state after execution.
func (j *JobBuilder) RetainState() *JobBuilder {
	j.retainState = true
	return j
}

// buildTags constructs tags for the job based on its configuration.
func (j *JobBuilder) buildTags(jobName string) []string {
	var tags []string

	// Always include environment
	currentEnv := getEnv("APP_ENV", "local")
	tags = append(tags, "env="+currentEnv)

	// Interval / cron info
	if j.cronExpr != "" {
		tags = append(tags, "cron="+j.cronExpr)
	}
	if j.duration != nil {
		tags = append(tags, "interval="+j.duration.String())
	}

	// Job name
	if jobName != "" {
		tags = append(tags, "name="+jobName)
	}

	// Args/flags provided via Command()
	if len(j.extraTags) > 0 {
		tags = append(tags, j.extraTags...)
	}

	return tags
}

// Do schedules the job with the provided task function.
func (j *JobBuilder) Do(task func()) *JobBuilder {
	if j.err != nil {
		return j
	}

	localHooks := j.hooks
	bg := j.runInBackground

	taskToRun := task
	if j.targetKind == jobTargetFunction {
		taskToRun = j.taskWithHooks(task, localHooks, bg)
	}

	if j.targetKind == "" {
		j.targetKind = jobTargetFunction
	}

	var jobName string
	if j.name != "" {
		jobName = j.name
	}

	var opts []gocron.JobOption
	if j.withoutOverlap {
		opts = append(opts, gocron.WithSingletonMode(gocron.LimitModeReschedule))
	}
	if j.distributedLocker != nil {
		opts = append(opts, gocron.WithDistributedJobLocker(j.distributedLocker))
	}
	if jobName != "" {
		opts = append(opts, gocron.WithName(jobName))
	}

	if len(j.envs) > 0 {
		current := getEnv("APP_ENV", "local")
		matched := false
		for _, e := range j.envs {
			if e == current {
				matched = true
				break
			}
		}
		if !matched {
			return j // skip scheduling this job in non-matching environment
		}
	}

	if j.whenFunc != nil && !j.whenFunc() {
		return j // don't job if condition fails
	}
	if j.skipFunc != nil && j.skipFunc() {
		return j // don't job if skip triggers
	}

	// Build tags
	tags := j.buildTags(jobName)
	if len(tags) > 0 {
		opts = append(opts, gocron.WithTags(tags...))
	}

	var job gocron.Job
	if j.cronExpr != "" {
		expr := j.cronExpr
		if j.timezone != "" && !strings.HasPrefix(expr, "CRON_TZ=") && !strings.HasPrefix(expr, "TZ=") {
			expr = fmt.Sprintf("CRON_TZ=%s %s", j.timezone, expr)
		}

		job, j.err = j.scheduler.NewJob(
			gocron.CronJob(expr, true),
			gocron.NewTask(taskToRun),
			opts...,
		)
		if j.err != nil {
			j.err = fmt.Errorf("failed to create job with cron expression %q: %w", expr, j.err)
			return j
		}
	} else if j.duration != nil {
		job, j.err = j.scheduler.NewJob(
			gocron.DurationJob(*j.duration),
			gocron.NewTask(taskToRun),
			opts...,
		)
		if j.err != nil {
			return j
		}
	} else {
		j.err = fmt.Errorf("no job defined before calling Do")
		return j
	}

	j.lastJob = job
	j.recordJob(job, task)

	// Graceful reset unless .RetainState() was called
	if !j.retainState {
		j.resetState()
	} else {
		j.retainState = false // reset after honoring once
		j.commandArgs = nil
		j.targetKind = jobTargetFunction
	}

	return j
}

func (j *JobBuilder) resetState() {
	j.duration = nil
	j.cronExpr = ""
	j.withoutOverlap = false
	j.distributedLocker = nil
	j.envs = nil
	j.whenFunc = nil
	j.skipFunc = nil
	j.commandArgs = nil
	j.targetKind = jobTargetFunction
	j.name = ""
	j.hooks = taskHooks{}
	j.runInBackground = false
}

// WithoutOverlapping ensures the job does not run concurrently.
func (j *JobBuilder) WithoutOverlapping() *JobBuilder {
	j.withoutOverlap = true
	return j
}

// Error returns the error if any occurred during job scheduling.
func (j *JobBuilder) Error() error {
	return j.err
}

// Cron sets the cron expression for the job.
func (j *JobBuilder) Cron(expr string) *JobBuilder {
	j.cronExpr = expr
	return j
}

// every sets the duration for the job to run at regular intervals.
func (j *JobBuilder) every(duration time.Duration) *JobBuilder {
	j.duration = &duration
	return j
}

// Every schedules a job to run every X seconds, minutes, or hours.
func (j *JobBuilder) Every(duration int) *FluentEvery {
	return &FluentEvery{
		base:     j,
		interval: duration,
	}
}

// FluentEvery is a wrapper for scheduling jobs at regular intervals.
type FluentEvery struct {
	interval int
	base     *JobBuilder
}

// Seconds schedules the job to run every X seconds.
func (fe *FluentEvery) Seconds() *JobBuilder {
	return fe.base.every(time.Duration(fe.interval) * time.Second)
}

// Minutes schedules the job to run every X minutes.
func (fe *FluentEvery) Minutes() *JobBuilder {
	return fe.base.every(time.Duration(fe.interval) * time.Minute)
}

// Hours schedules the job to run every X hours.
func (fe *FluentEvery) Hours() *JobBuilder {
	return fe.base.every(time.Duration(fe.interval) * time.Hour)
}

// EverySecond schedules the job to run every 1 second.
func (j *JobBuilder) EverySecond() *JobBuilder {
	return j.every(1 * time.Second)
}

// EveryTwoSeconds schedules the job to run every 2 seconds.
func (j *JobBuilder) EveryTwoSeconds() *JobBuilder {
	return j.every(2 * time.Second)
}

// EveryFiveSeconds schedules the job to run every 5 seconds.
func (j *JobBuilder) EveryFiveSeconds() *JobBuilder {
	return j.every(5 * time.Second)
}

// EveryTenSeconds schedules the job to run every 10 seconds.
func (j *JobBuilder) EveryTenSeconds() *JobBuilder {
	return j.every(10 * time.Second)
}

// EveryFifteenSeconds schedules the job to run every 15 seconds.
func (j *JobBuilder) EveryFifteenSeconds() *JobBuilder {
	return j.every(15 * time.Second)
}

// EveryTwentySeconds schedules the job to run every 20 seconds.
func (j *JobBuilder) EveryTwentySeconds() *JobBuilder {
	return j.every(20 * time.Second)
}

// EveryThirtySeconds schedules the job to run every 30 seconds.
func (j *JobBuilder) EveryThirtySeconds() *JobBuilder {
	return j.every(30 * time.Second)
}

// EveryMinute schedules the job to run every 1 minute.
func (j *JobBuilder) EveryMinute() *JobBuilder {
	return j.every(1 * time.Minute)
}

// EveryTwoMinutes schedules the job to run every 2 minutes.
func (j *JobBuilder) EveryTwoMinutes() *JobBuilder {
	return j.every(2 * time.Minute)
}

// EveryThreeMinutes schedules the job to run every 3 minutes.
func (j *JobBuilder) EveryThreeMinutes() *JobBuilder {
	return j.every(3 * time.Minute)
}

// EveryFourMinutes schedules the job to run every 4 minutes.
func (j *JobBuilder) EveryFourMinutes() *JobBuilder {
	return j.every(4 * time.Minute)
}

// EveryFiveMinutes schedules the job to run every 5 minutes.
func (j *JobBuilder) EveryFiveMinutes() *JobBuilder {
	return j.every(5 * time.Minute)
}

// EveryTenMinutes schedules the job to run every 10 minutes.
func (j *JobBuilder) EveryTenMinutes() *JobBuilder {
	return j.every(10 * time.Minute)
}

// EveryFifteenMinutes schedules the job to run every 15 minutes.
func (j *JobBuilder) EveryFifteenMinutes() *JobBuilder {
	return j.every(15 * time.Minute)
}

// EveryThirtyMinutes schedules the job to run every 30 minutes.
func (j *JobBuilder) EveryThirtyMinutes() *JobBuilder {
	return j.every(30 * time.Minute)
}

// Hourly schedules the job to run every hour.
func (j *JobBuilder) Hourly() *JobBuilder {
	return j.every(1 * time.Hour)
}

// HourlyAt schedules the job to run every hour at the specified minute.
func (j *JobBuilder) HourlyAt(minute int) *JobBuilder {
	return j.Cron(fmt.Sprintf("%d * * * *", minute))
}

// EveryOddHour schedules the job to run every odd-numbered hour at the specified minute.
func (j *JobBuilder) EveryOddHour(minute int) *JobBuilder {
	return j.Cron(fmt.Sprintf("%d 1-23/2 * * *", minute))
}

// EveryTwoHours schedules the job to run every two hours at the specified minute.
func (j *JobBuilder) EveryTwoHours(minute int) *JobBuilder {
	return j.Cron(fmt.Sprintf("%d */2 * * *", minute))
}

// EveryThreeHours schedules the job to run every three hours at the specified minute.
func (j *JobBuilder) EveryThreeHours(minute int) *JobBuilder {
	return j.Cron(fmt.Sprintf("%d */3 * * *", minute))
}

// EveryFourHours schedules the job to run every four hours at the specified minute.
func (j *JobBuilder) EveryFourHours(minute int) *JobBuilder {
	return j.Cron(fmt.Sprintf("%d */4 * * *", minute))
}

// EverySixHours schedules the job to run every six hours at the specified minute.
func (j *JobBuilder) EverySixHours(minute int) *JobBuilder {
	return j.Cron(fmt.Sprintf("%d */6 * * *", minute))
}

// Daily schedules the job to run once per day at midnight.
func (j *JobBuilder) Daily() *JobBuilder {
	return j.Cron("0 0 * * *")
}

// DailyAt schedules the job to run daily at a specific time (e.g., "13:00").
func (j *JobBuilder) DailyAt(hm string) *JobBuilder {
	hour, minute, err := parseHourMinute(hm)
	if err != nil {
		j.err = fmt.Errorf("invalid DailyAt format: %w", err)
		return j
	}
	return j.Cron(fmt.Sprintf("%d %d * * *", minute, hour))
}

// TwiceDaily schedules the job to run daily at two specified hours (e.g., 1 and 13).
func (j *JobBuilder) TwiceDaily(h1, h2 int) *JobBuilder {
	return j.Cron(fmt.Sprintf("0 %d,%d * * *", h1, h2))
}

// TwiceDailyAt schedules the job to run daily at two specified times (e.g., 1:15 and 13:15).
func (j *JobBuilder) TwiceDailyAt(h1, h2, m int) *JobBuilder {
	return j.Cron(fmt.Sprintf("%d %d,%d * * *", m, h1, h2))
}

// Weekly schedules the job to run once per week on Sunday at midnight.
func (j *JobBuilder) Weekly() *JobBuilder {
	return j.Cron("0 0 * * 0")
}

// WeeklyOn schedules the job to run weekly on a specific day of the week and time.
// Day uses 0 = Sunday through 6 = Saturday.
func (j *JobBuilder) WeeklyOn(day int, hm string) *JobBuilder {
	parts := strings.Split(hm, ":")
	if len(parts) != 2 {
		j.err = fmt.Errorf("invalid WeeklyOn format: expected HH:MM but got %s", hm)
		return j
	}
	hourStr := strings.TrimSpace(parts[0])
	minuteStr := strings.TrimSpace(parts[1])

	hour, err := strconv.Atoi(hourStr)
	if err != nil {
		j.err = fmt.Errorf("invalid hour in WeeklyOn time string %q: %w", hourStr, err)
		return j
	}
	minute, err := strconv.Atoi(minuteStr)
	if err != nil {
		j.err = fmt.Errorf("invalid minute in WeeklyOn time string %q: %w", minuteStr, err)
		return j
	}

	return j.Cron(fmt.Sprintf("%d %d * * %d", minute, hour, day))
}

// Monthly schedules the job to run on the first day of each month at midnight.
func (j *JobBuilder) Monthly() *JobBuilder {
	return j.Cron("0 0 1 * *")
}

// MonthlyOn schedules the job to run on a specific day of the month at a given time.
func (j *JobBuilder) MonthlyOn(day int, hm string) *JobBuilder {
	hour, minute, err := parseHourMinute(hm)
	if err != nil {
		j.err = fmt.Errorf("invalid MonthlyOn time format: %w", err)
		return j
	}
	return j.Cron(fmt.Sprintf("%d %d %d * *", minute, hour, day))
}

// TwiceMonthly schedules the job to run on two specific days of the month at the given time.
func (j *JobBuilder) TwiceMonthly(d1, d2 int, hm string) *JobBuilder {
	hour, minute, err := parseHourMinute(hm)
	if err != nil {
		j.err = fmt.Errorf("invalid TwiceMonthly time format: %w", err)
		return j
	}
	return j.Cron(fmt.Sprintf("%d %d %d,%d * *", minute, hour, d1, d2))
}

// LastDayOfMonth schedules the job to run on the last day of each month at a specific time.
func (j *JobBuilder) LastDayOfMonth(hm string) *JobBuilder {
	hour, minute, err := parseHourMinute(hm)
	if err != nil {
		j.err = fmt.Errorf("invalid LastDayOfMonth time format: %w", err)
		return j
	}
	return j.Cron(fmt.Sprintf("%d %d L * *", minute, hour))
}

// Quarterly schedules the job to run on the first day of each quarter at midnight.
func (j *JobBuilder) Quarterly() *JobBuilder {
	return j.Cron("0 0 1 1,4,7,10 *")
}

// QuarterlyOn schedules the job to run on a specific day of each quarter at a given time.
func (j *JobBuilder) QuarterlyOn(day int, hm string) *JobBuilder {
	hour, minute, err := parseHourMinute(hm)
	if err != nil {
		j.err = fmt.Errorf("invalid QuarterlyOn time format: %w", err)
		return j
	}
	return j.Cron(fmt.Sprintf("%d %d %d 1,4,7,10 *", minute, hour, day))
}

// Yearly schedules the job to run on January 1st every year at midnight.
func (j *JobBuilder) Yearly() *JobBuilder {
	return j.Cron("0 0 1 1 *")
}

// YearlyOn schedules the job to run every year on a specific month, day, and time.
func (j *JobBuilder) YearlyOn(month, day int, hm string) *JobBuilder {
	hour, minute, err := parseHourMinute(hm)
	if err != nil {
		j.err = fmt.Errorf("invalid YearlyOn time format: %w", err)
		return j
	}
	return j.Cron(fmt.Sprintf("%d %d %d %d *", minute, hour, day, month))
}

// Timezone sets a timezone string for the job (not currently applied to gocron Scheduler).
func (j *JobBuilder) Timezone(zone string) *JobBuilder {
	j.timezone = zone
	return j
}

// WithCommandRunner overrides command execution (default: exec.CommandContext).
func (j *JobBuilder) WithCommandRunner(r CommandRunner) *JobBuilder {
	if r != nil {
		j.commandRunner = r
	}
	return j
}

// WithNowFunc overrides current time (default: time.Now). Useful for tests.
func (j *JobBuilder) WithNowFunc(fn func() time.Time) *JobBuilder {
	if fn != nil {
		j.now = fn
	}
	return j
}

// DaysOfMonth schedules the job to run on specific days of the month at a given time.
func (j *JobBuilder) DaysOfMonth(days []int, hm string) *JobBuilder {
	hour, minute, err := parseHourMinute(hm)
	if err != nil {
		j.err = fmt.Errorf("invalid DaysOfMonth time format: %w", err)
		return j
	}
	var parts []string
	for _, d := range days {
		parts = append(parts, strconv.Itoa(d))
	}
	return j.Cron(fmt.Sprintf("%d %d %s * *", minute, hour, strings.Join(parts, ",")))
}

// CronExpr returns the cron expression string configured for this job.
func (j *JobBuilder) CronExpr() string {
	return j.cronExpr
}

// Job returns the last scheduled gocron.Job instance, if available.
func (j *JobBuilder) Job() gocron.Job {
	return j.lastJob
}

// WithoutOverlappingWithLocker ensures the job does not run concurrently across distributed systems using the provided locker.
func (j *JobBuilder) WithoutOverlappingWithLocker(locker gocron.Locker) *JobBuilder {
	j.withoutOverlap = true
	j.distributedLocker = locker
	return j
}

// Environments restricts job registration to specific environment names (e.g. "production", "staging").
func (j *JobBuilder) Environments(envs ...string) *JobBuilder {
	j.envs = envs
	return j
}

// When only schedules the job if the provided condition returns true.
func (j *JobBuilder) When(fn func() bool) *JobBuilder {
	j.addWhen(fn)
	return j
}

// Skip prevents scheduling the job if the provided condition returns true.
func (j *JobBuilder) Skip(fn func() bool) *JobBuilder {
	j.addSkip(fn)
	return j
}

// Name sets an explicit job name.
func (j *JobBuilder) Name(name string) *JobBuilder {
	j.name = name
	return j
}

// RunInBackground runs command/exec tasks in a goroutine.
func (j *JobBuilder) RunInBackground() *JobBuilder {
	j.runInBackground = true
	return j
}

// Before sets a hook to run before task execution.
func (j *JobBuilder) Before(fn func()) *JobBuilder {
	j.hooks.Before = fn
	return j
}

// After sets a hook to run after task execution.
func (j *JobBuilder) After(fn func()) *JobBuilder {
	j.hooks.After = fn
	return j
}

// OnSuccess sets a hook to run after successful task execution.
func (j *JobBuilder) OnSuccess(fn func()) *JobBuilder {
	j.hooks.OnSuccess = fn
	return j
}

// OnFailure sets a hook to run after failed task execution.
func (j *JobBuilder) OnFailure(fn func()) *JobBuilder {
	j.hooks.OnFailure = fn
	return j
}

func (j *JobBuilder) addWhen(fn func() bool) {
	if fn == nil {
		return
	}
	if j.whenFunc == nil {
		j.whenFunc = fn
		return
	}
	prev := j.whenFunc
	j.whenFunc = func() bool {
		return prev() && fn()
	}
}

func (j *JobBuilder) addSkip(fn func() bool) {
	if fn == nil {
		return
	}
	if j.skipFunc == nil {
		j.skipFunc = fn
		return
	}
	prev := j.skipFunc
	j.skipFunc = func() bool {
		return prev() || fn()
	}
}

// Weekdays limits the job to run only on weekdays (Mon-Fri).
func (j *JobBuilder) Weekdays() *JobBuilder {
	j.addWhen(func() bool {
		return isWeekday(j.now(), j.location())
	})
	return j
}

// Weekends limits the job to run only on weekends (Sat-Sun).
func (j *JobBuilder) Weekends() *JobBuilder {
	j.addWhen(func() bool {
		return isWeekend(j.now(), j.location())
	})
	return j
}

// Specific day helpers
func (j *JobBuilder) Sundays() *JobBuilder    { return j.days(time.Sunday) }
func (j *JobBuilder) Mondays() *JobBuilder    { return j.days(time.Monday) }
func (j *JobBuilder) Tuesdays() *JobBuilder   { return j.days(time.Tuesday) }
func (j *JobBuilder) Wednesdays() *JobBuilder { return j.days(time.Wednesday) }
func (j *JobBuilder) Thursdays() *JobBuilder  { return j.days(time.Thursday) }
func (j *JobBuilder) Fridays() *JobBuilder    { return j.days(time.Friday) }
func (j *JobBuilder) Saturdays() *JobBuilder  { return j.days(time.Saturday) }

// Days limits the job to a specific set of weekdays.
func (j *JobBuilder) Days(days ...time.Weekday) *JobBuilder {
	set := make(map[time.Weekday]struct{}, len(days))
	for _, d := range days {
		set[d] = struct{}{}
	}
	j.addWhen(func() bool {
		now := j.now().In(j.location())
		_, ok := set[now.Weekday()]
		return ok
	})
	return j
}

func (j *JobBuilder) days(day time.Weekday) *JobBuilder {
	return j.Days(day)
}

// Between limits the job to run between the provided HH:MM times (inclusive).
func (j *JobBuilder) Between(start, end string) *JobBuilder {
	startH, startM, err := parseHourMinute(start)
	if err != nil {
		j.err = fmt.Errorf("invalid Between start time: %w", err)
		return j
	}
	endH, endM, err := parseHourMinute(end)
	if err != nil {
		j.err = fmt.Errorf("invalid Between end time: %w", err)
		return j
	}
	j.addWhen(func() bool {
		loc := j.location()
		now := j.now().In(loc)
		return timeInRange(now, startH, startM, endH, endM)
	})
	return j
}

// UnlessBetween prevents the job from running between the provided HH:MM times.
func (j *JobBuilder) UnlessBetween(start, end string) *JobBuilder {
	startH, startM, err := parseHourMinute(start)
	if err != nil {
		j.err = fmt.Errorf("invalid UnlessBetween start time: %w", err)
		return j
	}
	endH, endM, err := parseHourMinute(end)
	if err != nil {
		j.err = fmt.Errorf("invalid UnlessBetween end time: %w", err)
		return j
	}
	j.addSkip(func() bool {
		loc := j.location()
		now := j.now().In(loc)
		return timeInRange(now, startH, startM, endH, endM)
	})
	return j
}

// Command executes the current binary with the given subcommand and variadic args.
// Example: .Command("jobs:purge", "--force") → ./app jobs:purge --force
func (j *JobBuilder) Command(subcommand string, args ...string) *JobBuilder {
	j.name = subcommand
	j.targetKind = jobTargetCommand
	j.commandArgs = args
	localHooks := j.hooks
	bg := j.runInBackground

	// turn CLI args into tags
	if len(args) > 0 {
		// collapse into one quoted string
		joined := strings.Join(args, " ")
		j.extraTags = []string{fmt.Sprintf("args=\"%s\"", joined)}
	}

	task := func() {
		if j.err != nil {
			fmt.Printf("❌ Error scheduling command: %v\n", j.err)
			return
		}

		exe, err := os.Executable()
		if err != nil {
			fmt.Printf("Unable to determine executable path: %v\n", err)
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
		defer cancel()

		cmdArgs := append([]string{subcommand}, args...)
		run := func() error {
			if os.Getenv("SCHEDULER_TEST_NO_EXEC") != "1" {
				return j.commandRunner.Run(ctx, exe, cmdArgs)
			}
			return j.commandRunner.Run(ctx, exe, cmdArgs)
		}

		j.runWithHooks(run, localHooks, bg)
	}

	return j.Do(task)
}

// JobMetadata returns a copy of the tracked job metadata keyed by job ID.
func (j *JobBuilder) JobMetadata() map[uuid.UUID]jobMetadata {
	out := make(map[uuid.UUID]jobMetadata, len(j.jobMetadata))
	for id, meta := range j.jobMetadata {
		out[id] = meta
	}
	return out
}

func (j *JobBuilder) recordJob(job gocron.Job, task func()) {
	if j.jobMetadata == nil {
		j.jobMetadata = make(map[uuid.UUID]jobMetadata)
	}

	scheduleType, schedule := j.describeSchedule()
	kind := j.targetKind
	if kind == "" {
		kind = jobTargetFunction
	}

	meta := jobMetadata{
		id:           job.ID(),
		name:         job.Name(),
		schedule:     schedule,
		scheduleType: scheduleType,
		targetKind:   kind,
		tags:         job.Tags(),
	}

	if kind == jobTargetCommand {
		meta.command = buildCommandString(j.name, j.commandArgs)
		if meta.name == "" {
			meta.name = j.name
		}
	} else {
		meta.handler = friendlyFuncName(task)
		if meta.name == "" && j.name != "" {
			meta.name = j.name
		}
	}

	j.jobMetadata[meta.id] = meta
}

func (j *JobBuilder) describeSchedule() (jobScheduleKind, string) {
	switch {
	case j.cronExpr != "":
		if j.timezone != "" {
			return jobScheduleCron, fmt.Sprintf("CRON_TZ=%s %s", j.timezone, j.cronExpr)
		}
		return jobScheduleCron, j.cronExpr
	case j.duration != nil:
		return jobScheduleInterval, j.duration.String()
	default:
		return jobScheduleUnknown, ""
	}
}

func buildCommandString(name string, args []string) string {
	if name == "" {
		return ""
	}
	if len(args) == 0 {
		return name
	}
	return strings.TrimSpace(name + " " + strings.Join(args, " "))
}

func friendlyFuncName(fn func()) string {
	if fn == nil {
		return ""
	}

	ptr := reflect.ValueOf(fn).Pointer()
	rf := runtime.FuncForPC(ptr)
	if rf == nil {
		return ""
	}

	name := rf.Name()
	name = strings.TrimSuffix(name, "-fm")

	anon := false
	funcRe := regexp.MustCompile(`\.func\d+`)
	if funcRe.MatchString(name) {
		anon = true
	}
	name = funcRe.ReplaceAllString(name, "")

	name = filepath.ToSlash(name)
	if idx := strings.LastIndex(name, "/"); idx != -1 && idx+1 < len(name) {
		name = name[idx+1:]
	}

	name = strings.ReplaceAll(name, "(*", "")
	name = strings.ReplaceAll(name, ")", "")
	name = strings.ReplaceAll(name, "..", ".")
	name = strings.TrimPrefix(name, ".")

	segments := strings.Split(name, ".")
	if len(segments) >= 3 {
		n := len(segments)
		typePart := segments[n-3]
		method := segments[n-2]
		anonSuffix := ""
		if anon {
			anonSuffix = " (anon func)"
		}
		return strings.TrimSpace(fmt.Sprintf("%s.%s%s", typePart, method, anonSuffix))
	}
	if len(segments) == 2 {
		out := strings.Join(segments, ".")
		if anon {
			return out + " (anon func)"
		}
		return out
	}
	if len(segments) == 1 {
		out := segments[0]
		if anon {
			return out + " (anon func)"
		}
		return out
	}
	return name
}

func (j *JobBuilder) location() *time.Location {
	if j.timezone == "" {
		return time.Local
	}
	loc, err := time.LoadLocation(j.timezone)
	if err != nil {
		return time.Local
	}
	return loc
}

func (j *JobBuilder) runWithHooks(run func() error, hooks taskHooks, bg bool) {
	if hooks.Before != nil {
		hooks.Before()
	}

	execFn := func() {
		err := run()
		if err != nil {
			if hooks.OnFailure != nil {
				hooks.OnFailure()
			}
		} else {
			if hooks.OnSuccess != nil {
				hooks.OnSuccess()
			}
		}
		if hooks.After != nil {
			hooks.After()
		}
	}

	if bg {
		go execFn()
		return
	}
	execFn()
}

func (j *JobBuilder) taskWithHooks(fn func(), hooks taskHooks, bg bool) func() {
	return func() {
		j.runWithHooks(func() error {
			fn()
			return nil
		}, hooks, bg)
	}
}

func isWeekday(t time.Time, loc *time.Location) bool {
	w := t.In(loc).Weekday()
	return w >= time.Monday && w <= time.Friday
}

func isWeekend(t time.Time, loc *time.Location) bool {
	w := t.In(loc).Weekday()
	return w == time.Saturday || w == time.Sunday
}

func timeInRange(now time.Time, startH, startM, endH, endM int) bool {
	loc := now.Location()
	start := time.Date(now.Year(), now.Month(), now.Day(), startH, startM, 0, 0, loc)
	end := time.Date(now.Year(), now.Month(), now.Day(), endH, endM, 0, 0, loc)

	if end.Before(start) {
		// crosses midnight: treat as two ranges (start..23:59) or (00:00..end)
		if !now.Before(start) {
			return true
		}
		tomorrow := time.Date(now.Year(), now.Month(), now.Day()+1, endH, endM, 0, 0, loc)
		return !now.After(tomorrow)
	}

	return (now.Equal(start) || now.After(start)) && (now.Equal(end) || now.Before(end))
}

func getEnv(key, def string) string {
	val := os.Getenv(key)
	if val == "" {
		return def
	}
	return val
}

// parseHourMinute parses a string in the format "HH:MM" and returns the hour and minute as integers.
func parseHourMinute(hm string) (int, int, error) {
	parts := strings.Split(hm, ":")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid time format (expected HH:MM): %q", hm)
	}
	hour, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return 0, 0, fmt.Errorf("invalid hour: %w", err)
	}
	minute, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return 0, 0, fmt.Errorf("invalid minute: %w", err)
	}
	return hour, minute, nil
}

// redisLockerClient is the minimal interface we need from a Redis client.
type redisLockerClient interface {
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
}

// RedisLocker is a simple gocron Locker backed by redis NX locks.
// It uses a single key per job and auto-expires after ttl.
type RedisLocker struct {
	client redisLockerClient
	ttl    time.Duration
}

// NewRedisLocker creates a RedisLocker with a client and TTL.
func NewRedisLocker(client redisLockerClient, ttl time.Duration) *RedisLocker {
	return &RedisLocker{client: client, ttl: ttl}
}

// Lock obtains a lock for the job name.
func (l *RedisLocker) Lock(ctx context.Context, key string) (gocron.Lock, error) {
	locked, err := l.client.SetNX(ctx, l.lockKey(key), "1", l.ttl).Result()
	if err != nil {
		return nil, err
	}
	if !locked {
		return nil, errLockNotAcquired
	}
	return &redisLock{
		client: l.client,
		key:    l.lockKey(key),
	}, nil
}

func (l *RedisLocker) lockKey(name string) string {
	return "gocron:lock:" + name
}

// Internal lock and error helpers.
type redisLock struct {
	client redisLockerClient
	key    string
}

func (l *redisLock) Unlock(ctx context.Context) error {
	_, err := l.client.Del(ctx, l.key).Result()
	return err
}

var errLockNotAcquired = &lockError{"could not acquire lock"}

type lockError struct {
	msg string
}

func (e *lockError) Error() string { return e.msg }
