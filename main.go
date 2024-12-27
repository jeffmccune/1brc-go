package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/jeffmccune/1brc-go/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type Task interface {
	Run(ctx context.Context) (StationMap, error)
}

var _ Task = &TaskV1{}

type TaskV1 struct {
	id  int
	buf []byte
}

func (r *TaskV1) Run(ctx context.Context) (StationMap, error) {
	sm := make(StationMap, 10_000)
	buf := bytes.NewBuffer(r.buf)

	for {
		line, err := buf.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				return sm, nil
			}
			return sm, errors.Wrap(err)
		}
		// split into station name and measurement
		idx := bytes.Index(line, []byte(";"))
		station := string(line[:idx])
		// Parse the integer part, skip the newline at the end.
		temp, err := strconv.ParseInt(string(line[idx+1:len(line)-3]), 10, 16)
		if err != nil {
			return sm, errors.Wrap(err)
		}
		// Parse the fractional part, skip the newline at the end.
		fractional, err := strconv.ParseUint(string(line[len(line)-2]), 10, 16)
		if err != nil {
			return sm, errors.Wrap(err)
		}
		// the final temperature multiplied by 10
		if temp > 0 {
			temp = temp*10 + int64(fractional)
		} else {
			temp = temp*10 - int64(fractional)
		}

		// Update the station map.
		sm.Add(station, temp)
	}
}

type Temperature struct {
	min, max, sum, num int64
}

func (t *Temperature) Add(val int64) {
	t.min = min(t.min, val)
	t.max = max(t.max, val)
	t.sum += val
	t.num += 1
}

func (t *Temperature) Merge(other *Temperature) {
	t.min = min(t.min, other.min)
	t.max = max(t.max, other.max)
	t.sum += other.sum
	t.num += other.num
}

func (t Temperature) String() string {
	return fmt.Sprintf(
		"%.1f/%.1f/%.1f",
		0.1*float64(t.min),
		0.1*float64(t.sum/t.num),
		0.1*float64(t.max),
	)
}

type StationMap map[string]*Temperature

func (sm StationMap) Merge(other StationMap) {
	for station, temp := range other {
		if t, ok := sm[station]; ok {
			t.Merge(temp)
		} else {
			sm[station] = temp
		}
	}
}

func (sm StationMap) Add(station string, val int64) {
	if t, ok := sm[station]; ok {
		t.Add(val)
	} else {
		sm[station] = &Temperature{
			min: val,
			max: val,
			sum: val,
			num: 1,
		}
	}
}

func main() {
	profile := flag.Bool("profile", false, "enable profiling")
	flag.Parse()

	if *profile {
		f, err := os.Create("default.pgo")
		if err != nil {
			panic(err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			panic(err)
		}
		defer pprof.StopCPUProfile()
	}

	err := Run(context.Background(), os.Stdin)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
	}
}

func Run(ctx context.Context, r io.Reader) error {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)

	tasks := make(chan Task)
	results := make(chan StationMap)

	// Consumers
	var wg sync.WaitGroup
	for id := 0; id < runtime.NumCPU(); id++ {
		wg.Add(1)
		g.Go(func() error {
			defer wg.Done()
			return Consumer(ctx, id, tasks, results)
		})
	}

	// Producer
	g.Go(func() error { return Producer(ctx, tasks, r) })

	// Collector
	stationMap := make(StationMap, 10_000)
	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return errors.Wrap(ctx.Err())
			case sm, ok := <-results:
				if !ok {
					return nil
				}
				stationMap.Merge(sm)
			}
		}
	})

	// Release the collector once all consumers have submitted results.
	g.Go(func() error {
		wg.Wait()
		close(results)
		return nil
	})

	if err := g.Wait(); err != nil {
		return errors.Wrap(err)
	}

	if err := Summary(ctx, stationMap, os.Stdout); err != nil {
		return errors.Wrap(err)
	}

	return g.Wait()
}

func Consumer(ctx context.Context, id int, tasks chan Task, results chan StationMap) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err())
		case task, ok := <-tasks:
			if !ok {
				// producer closed the tasks channel, stop the consumer.
				return nil
			}
			sm, err := task.Run(ctx)
			if err != nil {
				return errors.Wrap(err)
			}
			select {
			case <-ctx.Done():
				return errors.Wrap(ctx.Err())
			case results <- sm:
			}
		}
	}
}

// ReadChunk reads a size chunk of data, with head and tail split on the last
// newline.
func ReadChunk(partial []byte, size int, r io.Reader) (head, tail []byte, err error) {
	buf := make([]byte, size)
	n, err := r.Read(buf)
	if n == 0 {
		return nil, partial, err
	}

	combined := partial
	combined = append(combined, buf[:n]...)

	idx := bytes.LastIndexByte(combined, '\n')

	if idx == -1 {
		return nil, combined, errors.Format("no newline found in %d bytes", len(combined))
	}

	head = combined[:idx+1]
	tail = combined[idx+1:]
	return
}

// Producer produces tasks with complete buffers ending in a newline.
func Producer(ctx context.Context, tasks chan Task, r io.Reader) error {
	defer close(tasks)
	var id int
	var tail []byte

	for {
		head, newTail, err := ReadChunk(tail, 1024*1024*4, os.Stdin)
		if err != nil {
			if err == io.EOF {
				break
			}
		}

		select {
		case tasks <- &TaskV1{id: id, buf: head}:
		case <-ctx.Done():
			return errors.Wrap(ctx.Err())
		}
		tail = newTail
		id += 1
	}

	// Handle remaining data
	if len(tail) > 0 {
		select {
		case tasks <- &TaskV1{buf: tail}:
		case <-ctx.Done():
			return errors.Wrap(ctx.Err())
		}
	}

	return nil
}

// emit the results on stdout like this (i.e. sorted alphabetically by station
// name, and the result values per station in the format <min>/<mean>/<max>,
// rounded to one fractional digit)
func Summary(ctx context.Context, sm StationMap, w io.Writer) error {
	stations := make([]string, 0, 10000)
	for station := range sm {
		stations = append(stations, station)
	}
	sort.Strings(stations)
	for _, station := range stations {
		temperature, _ := sm[station]
		fmt.Fprintf(w, "%s=%s\n", station, temperature.String())
	}
	return nil
}
