package engine

import (
	"bytes"
	"fmt"
	"github.com/golang/glog"
	"io"
	"strings"
	"sync"
	"time"
)

// A job is the fundamental unit of work in the docker engine.
// Everything docker can do should eventually be exposed as a job.
// For example: execute a process in a container, create a new container,
// download an archive from the internet, serve the http api, etc.
//
// The job API is designed after unix processes: a job has a name, arguments,
// environment variables, standard streams for input, output and error.
type Job struct {
	Eng     *Engine
	Name    string
	Args    []string
	env     *Env
	Stdout  *Output
	Stderr  *Output
	Stdin   *Input
	handler Handler
	end     time.Time
	closeIO bool

	// When closed, the job has been cancelled.
	// Note: not all jobs implement cancellation.
	// See Job.Cancel() and Job.WaitCancelled()
	cancelled  chan struct{}
	cancelOnce sync.Once
}

// Run executes the job and blocks until the job completes.
// If the job fails it returns an error
func (job *Job) Run() (err error) {
	defer func() {
		// Wait for all background tasks to complete
		if job.closeIO {
			if err := job.Stdout.Close(); err != nil {
				glog.Errorf("%v", err)
			}
			if err := job.Stderr.Close(); err != nil {
				glog.Errorf("%v", err)
			}
			if err := job.Stdin.Close(); err != nil {
				glog.Errorf("%v", err)
			}
		}
	}()

	if job.Eng.IsShutdown() && !job.GetenvBool("overrideShutdown") {
		return fmt.Errorf("engine is shutdown")
	}
	// FIXME: this is a temporary workaround to avoid Engine.Shutdown
	// waiting 5 seconds for server/api.ServeApi to complete (which it never will)
	// everytime the daemon is cleanly restarted.
	// The permanent fix is to implement Job.Stop and Job.OnStop so that
	// ServeApi can cooperate and terminate cleanly.
	if job.Name != "serveapi" {
		job.Eng.l.Lock()
		job.Eng.tasks.Add(1)
		job.Eng.l.Unlock()
		defer job.Eng.tasks.Done()
	}
	// FIXME: make this thread-safe
	// FIXME: implement wait
	if !job.end.IsZero() {
		return fmt.Errorf("%s: job has already completed", job.Name)
	}
	// Log beginning and end of the job
	if job.Eng.Logging {
		glog.V(0).Infof("+job %s", job.CallString())
		defer func() {
			okerr := "OK"
			if err != nil {
				okerr = fmt.Sprintf("ERR: %s", err)
			}
			glog.V(0).Infof("-job %s %s", job.CallString(), okerr)
		}()
	}

	if job.handler == nil {
		return fmt.Errorf("%s: command not found", job.Name)
	}

	var errorMessage = bytes.NewBuffer(nil)
	job.Stderr.Add(errorMessage)

	err = job.handler(job)
	job.end = time.Now()

	return
}

func (job *Job) CallString() string {
	return fmt.Sprintf("%s(%s)", job.Name, strings.Join(job.Args, ", "))
}

func (job *Job) Env() *Env {
	return job.env
}

func (job *Job) EnvExists(key string) (value bool) {
	return job.env.Exists(key)
}

func (job *Job) Getenv(key string) (value string) {
	return job.env.Get(key)
}

func (job *Job) GetenvBool(key string) (value bool) {
	return job.env.GetBool(key)
}

func (job *Job) SetenvBool(key string, value bool) {
	job.env.SetBool(key, value)
}

func (job *Job) GetenvTime(key string) (value time.Time, err error) {
	return job.env.GetTime(key)
}

func (job *Job) SetenvTime(key string, value time.Time) {
	job.env.SetTime(key, value)
}

func (job *Job) GetenvSubEnv(key string) *Env {
	return job.env.GetSubEnv(key)
}

func (job *Job) SetenvSubEnv(key string, value *Env) error {
	return job.env.SetSubEnv(key, value)
}

func (job *Job) GetenvInt64(key string) int64 {
	return job.env.GetInt64(key)
}

func (job *Job) GetenvInt(key string) int {
	return job.env.GetInt(key)
}

func (job *Job) SetenvInt64(key string, value int64) {
	job.env.SetInt64(key, value)
}

func (job *Job) SetenvInt(key string, value int) {
	job.env.SetInt(key, value)
}

// Returns nil if key not found
func (job *Job) GetenvList(key string) []string {
	return job.env.GetList(key)
}

func (job *Job) GetenvJson(key string, iface interface{}) error {
	return job.env.GetJson(key, iface)
}

func (job *Job) SetenvJson(key string, value interface{}) error {
	return job.env.SetJson(key, value)
}

func (job *Job) SetenvList(key string, value []string) error {
	return job.env.SetJson(key, value)
}

func (job *Job) Setenv(key, value string) {
	job.env.Set(key, value)
}

// DecodeEnv decodes `src` as a json dictionary, and adds
// each decoded key-value pair to the environment.
//
// If `src` cannot be decoded as a json dictionary, an error
// is returned.
func (job *Job) DecodeEnv(src io.Reader) error {
	return job.env.Decode(src)
}

func (job *Job) EncodeEnv(dst io.Writer) error {
	return job.env.Encode(dst)
}

func (job *Job) ImportEnv(src interface{}) (err error) {
	return job.env.Import(src)
}

func (job *Job) Environ() map[string]string {
	return job.env.Map()
}

func (job *Job) Printf(format string, args ...interface{}) (n int, err error) {
	return fmt.Fprintf(job.Stdout, format, args...)
}

func (job *Job) Errorf(format string, args ...interface{}) (n int, err error) {
	return fmt.Fprintf(job.Stderr, format, args...)
}

func (job *Job) SetCloseIO(val bool) {
	job.closeIO = val
}

// When called, causes the Job.WaitCancelled channel to unblock.
func (job *Job) Cancel() {
	job.cancelOnce.Do(func() {
		close(job.cancelled)
	})
}

// Returns a channel which is closed ("never blocks") when the job is cancelled.
func (job *Job) WaitCancelled() <-chan struct{} {
	return job.cancelled
}
