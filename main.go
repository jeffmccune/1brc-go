package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jeffmccune/1brc-go/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type measurement struct {
	mu    *sync.Mutex
	min   float64
	max   float64
	total float64
	count int
}

func (m *measurement) Update(temperature float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.min = min(m.min, temperature)
	m.max = max(m.max, temperature)
	m.total += temperature
	m.count += 1
}

func NewMeasurement(temperature float64) *measurement {
	return &measurement{
		mu:    &sync.Mutex{},
		min:   temperature,
		max:   temperature,
		total: temperature,
		count: 1,
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
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)
	tasks := make(chan string)

	syncMap := sync.Map{}

	// Consumers
	for i := 0; i < runtime.NumCPU(); i++ {
		g.Go(func() error { return Consumer(ctx, tasks, &syncMap) })
	}

	// Producer
	g.Go(func() error { return Producer(ctx, tasks, r) })

	if err := g.Wait(); err != nil {
		return errors.Wrap(err)
	}

	if err := Summary(ctx, &syncMap, os.Stdout); err != nil {
		return errors.Wrap(err)
	}

	return g.Wait()
}

func Consumer(ctx context.Context, tasks chan string, syncMap *sync.Map) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err())
		case s, ok := <-tasks:
			if !ok {
				// producer closed the tasks channel, stop the consumer.
				return nil
			}
			rec := strings.SplitN(s, ";", 2)
			if len(rec) != 2 {
				continue // invalid record, can happen on the final newline.
			}
			temp, err := strconv.ParseFloat(rec[1], 64)
			if err != nil {
				return errors.Wrap(err)
			}

			m := NewMeasurement(temp)
			val, loaded := syncMap.LoadOrStore(rec[0], m)
			if loaded {
				m = val.(*measurement)
				m.Update(temp)
			}
		}
	}
}

func Producer(ctx context.Context, tasks chan string, r io.Reader) error {
	defer close(tasks)

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		select {
		case tasks <- scanner.Text():
		case <-ctx.Done():
			return errors.Wrap(ctx.Err())
		}
	}
	if err := scanner.Err(); err != nil {
		return errors.Wrap(err)
	}
	return nil
}

// emit the results on stdout like this (i.e. sorted alphabetically by station
// name, and the result values per station in the format <min>/<mean>/<max>,
// rounded to one fractional digit)
func Summary(ctx context.Context, syncMap *sync.Map, w io.Writer) error {
	stations := make([]string, 0, 10000)
	syncMap.Range(func(station, v any) bool {
		stations = append(stations, station.(string))
		return true
	})
	sort.Strings(stations)
	for _, station := range stations {
		v, ok := syncMap.Load(station)
		if !ok {
			return errors.Format("station %s not found", station)
		}
		m := v.(*measurement)
		fmt.Fprintf(w, "%s=%.1f/%.1f/%.1f\n", station, m.min, m.total/float64(m.count), m.max)
	}
	return nil
}
