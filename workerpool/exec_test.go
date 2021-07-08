package workerpool

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"
)

const (
	jobsCount   = 10
	workerCount = 2
)

func TestWorkerPool(t *testing.T) {
	wp := New(workerCount)

	ctx, cancel := context.WithCancel(context.TODO())
	defer cancel()

	go wp.GenerateFrom(testJobs())

	go wp.Run(ctx)

	for {
		select {
		case r, ok := <-wp.Results():
			if !ok {
				continue
			}

			i, err := strconv.ParseInt(string(r.Descriptor.ID), 10, 64)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			val := r.Value.(int)
			expectedVal := int(i) * 2
			if val != expectedVal {
				t.Fatalf("wrong value %v; expected %v", val, expectedVal)
			}
		case <-wp.Done:
			return
		default:

		}
	}
}

func TestWorkerPool_Timeout(t *testing.T) {
	wp := New(workerCount)

	// create a context which times out before the job can be executed
	ctx, cancel := context.WithTimeout(context.TODO(), time.Nanosecond*10)
	defer cancel()

	go wp.Run(ctx)

	for {
		select {
		case r := <-wp.Results():
			if r.Err != nil && r.Err != context.DeadlineExceeded {
				t.Fatalf("expected error: %v; got: %v", context.DeadlineExceeded, r.Err)
			}
		case <-wp.Done:
			return
		default:
		}
	}
}

func TestWorkerPool_Cancel(t *testing.T) {
	wp := New(workerCount)

	ctx, cancel := context.WithCancel(context.TODO())

	// trigger worker pool then cancel immediately
	go wp.Run(ctx)
	cancel()

	for {
		select {
		case r := <-wp.Results():
			if r.Err != nil && r.Err != context.Canceled {
				t.Fatalf("expected error: %v; got: %v", context.Canceled, r.Err)
			}
		case <-wp.Done:
			return
		default:
		}
	}
}

func testJobs() []Job {
	jobs := make([]Job, jobsCount)
	for i := 0; i < jobsCount; i++ {
		jobs[i] = Job{
			Descriptor: JobDescriptor{
				ID:       JobID(fmt.Sprintf("%v", i)),
				JType:    "anyType",
				Metadata: nil,
			},
			ExecFn: execFn,
			Args:   i,
		}
	}

	return jobs
}
