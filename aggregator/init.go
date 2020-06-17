package aggregator

import (
	"log"
	"math"
	"os"
	"sync"
	"time"

	"github.com/Dieterbe/go-metrics"
	"github.com/grafana/carbon-relay-ng/stats"
	"github.com/grafana/carbon-relay-ng/util"
)

var numTooOld metrics.Counter
var rangeTracker *RangeTracker
var flushes = util.NewLimiter(1)
var flushWaiting = stats.Gauge("unit=aggregator.what=flush_waiting")
var debugLogEnabled bool

var aggregatorReporter *AggregatorReporter

func InitMetrics() {
	numTooOld = stats.Counter("module=aggregator.unit=Metric.what=TooOld")
	rangeTracker = NewRangeTracker()
}

func InitDebugLog(filename string) (cleanup func(), err error) {
	debugLogEnabled = true
	aggLogFile, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return func() {}, err
	}
	log.SetOutput(aggLogFile)
	return func() {
		aggLogFile.Close()
	}, nil
}

type RangeTracker struct {
	sync.Mutex
	min  uint32
	max  uint32
	minG metrics.Gauge
	maxG metrics.Gauge
}

func NewRangeTracker() *RangeTracker {
	m := &RangeTracker{
		min:  math.MaxUint32,
		minG: stats.Gauge("module=aggregator.unit=s.what=timestamp_received.type=min"),
		maxG: stats.Gauge("module=aggregator.unit=s.what=timestamp_received.type=max"),
	}
	go m.Run()
	return m
}

func (m *RangeTracker) Run() {
	for now := range time.Tick(time.Second) {
		m.Lock()
		min := m.min
		max := m.max
		m.min = math.MaxUint32
		m.max = 0
		m.Unlock()

		// if we have not seen any value yet, just report "in sync"
		if max == 0 {
			min = uint32(now.Unix())
			max = min
		}

		m.minG.Update(int64(min))
		m.maxG.Update(int64(max))
	}
}

func (m *RangeTracker) Sample(ts uint32) {
	m.Lock()
	if ts > m.max {
		m.max = ts
	}
	if ts < m.min {
		m.min = ts
	}
	m.Unlock()
}
