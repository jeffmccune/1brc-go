package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
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

	if r.buf[len(r.buf)-1] != '\n' {
		return sm, errors.Format("task %d cannot run: buffer with len %d does not end in a newline", r.id, len(r.buf))
	}

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
		// up to the ;
		station := string(line[:idx])
		// after the ; up to the final newline byte.
		temperature := line[idx+1 : len(line)-1]

		// This is only about 1 second slower than parsing it as an int.
		temp := ParseTemp(temperature)

		// Update the station map.
		sm.Add(station, temp)
	}
}

type Temperature struct {
	min, max int16
	num      int32
	sum      int64
}

func (t *Temperature) Add(val int16) {
	t.min = min(t.min, val)
	t.max = max(t.max, val)
	t.sum += int64(val)
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
		float64(t.min)/10,
		math.Round(float64(t.sum)/float64(t.num))/10,
		float64(t.max)/10,
	)
}

func ParseTemp(numb []byte) (num int16) {
	var negative bool
	if numb[0] == '-' {
		negative = true
		numb = numb[1:]
	}
	for _, c := range numb {
		if c == '.' {
			continue
		}
		num = num*10 + int16(c-'0')
	}
	if negative {
		return -num
	}
	return num
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

func (sm StationMap) Add(station string, val int16) {
	// TODO: Could change station to a []byte and use
	// unsafe.String(unsafe.SliceData(buf), len(buf)) to read from the map without
	// an allocation.
	if t, ok := sm[station]; ok {
		t.Add(val)
	} else {
		sm[station] = &Temperature{
			min: val,
			max: val,
			sum: int64(val),
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
			} else {
				return errors.Wrap(err)
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
	fmt.Fprint(w, "{")
	sep := ""
	for _, station := range stations {
		temperature, _ := sm[station]
		fmt.Fprintf(w, "%s%s=%s", sep, station, temperature.String())
		sep = ", "
	}
	fmt.Fprint(w, "}\n")
	return nil
}
