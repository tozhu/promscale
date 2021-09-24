// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgtype"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/common/errors"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/prompb"
	"go.opentelemetry.io/collector/model/pdata"
)

type Cfg struct {
	AsyncAcks              bool
	ReportInterval         int
	NumCopiers             int
	DisableEpochSync       bool
	IgnoreCompressedChunks bool
}

// DBIngestor ingest the TimeSeries data into Timescale database.
type DBIngestor struct {
	sCache     cache.SeriesCache
	dispatcher model.Dispatcher
}

// NewPgxIngestor returns a new Ingestor that uses connection pool and a metrics cache
// for caching metric table names.
func NewPgxIngestor(conn pgxconn.PgxConn, cache cache.MetricCache, sCache cache.SeriesCache, eCache cache.PositionCache, cfg *Cfg) (*DBIngestor, error) {
	dispatcher, err := newPgxDispatcher(conn, cache, sCache, eCache, cfg)
	if err != nil {
		return nil, err
	}
	return &DBIngestor{
		sCache:     sCache,
		dispatcher: dispatcher,
	}, nil
}

// NewPgxIngestorForTests returns a new Ingestor that write to PostgreSQL using PGX
// with an empty config, a new default size metrics cache and a non-ha-aware data parser
func NewPgxIngestorForTests(conn pgxconn.PgxConn, cfg *Cfg) (*DBIngestor, error) {
	if cfg == nil {
		cfg = &Cfg{}
	}
	cacheConfig := cache.DefaultConfig
	c := cache.NewMetricCache(cacheConfig)
	s := cache.NewSeriesCache(cacheConfig, nil)
	e := cache.NewExemplarLabelsPosCache(cacheConfig)
	return NewPgxIngestor(conn, c, s, e, cfg)
}

const (
	meta = iota
	series
)

// result contains insert stats and is used when we ingest samples and metadata concurrently.
type result struct {
	id      int8
	numRows uint64
	err     error
}

func (ingestor *DBIngestor) IngestTraces(ctx context.Context, r pdata.Traces) error {
	rSpans := r.ResourceSpans()
	for i := 0; i < rSpans.Len(); i++ {
		rSpan := rSpans.At(i)

		var rSchemaURLID pgtype.Int8
		var err error
		rSchemaURLID, err = ingestor.dispatcher.InsertSchemaURL(ctx, rSpan.SchemaUrl())
		if err != nil {
			return err
		}

		instLibSpans := rSpan.InstrumentationLibrarySpans()
		for j := 0; j < instLibSpans.Len(); j++ {
			instLibSpan := instLibSpans.At(j)
			instLib := instLibSpan.InstrumentationLibrary()
			instLibID, err := ingestor.dispatcher.InsertInstrumentationLibrary(ctx, instLib.Name(), instLib.Version(), instLibSpan.SchemaUrl())
			if err != nil {
				return err
			}

			spans := instLibSpan.Spans()

			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)

				nameID, err := ingestor.dispatcher.InsertSpanName(ctx, span.Name())
				if err != nil {
					return err
				}
				if err = ingestor.dispatcher.InsertSpanEvents(ctx, span.Events(), span.TraceID().Bytes(), span.SpanID().Bytes()); err != nil {
					return err
				}
				if err = ingestor.dispatcher.InsertSpanLinks(ctx, span.Links(), span.TraceID().Bytes(), span.SpanID().Bytes(), span.StartTimestamp().AsTime()); err != nil {
					return err
				}
				if err = ingestor.dispatcher.InsertSpan(ctx, span, nameID, instLibID, rSchemaURLID, rSpan.Resource().Attributes()); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// Ingest transforms and ingests the timeseries data into Timescale database.
// input:
//     tts the []Timeseries to insert
//     req the WriteRequest backing tts. It will be added to our WriteRequest
//         pool when it is no longer needed.
func (ingestor *DBIngestor) Ingest(r *prompb.WriteRequest) (numInsertablesIngested uint64, numMetadataIngested uint64, err error) {
	activeWriteRequests.Inc()
	defer activeWriteRequests.Dec() // Dec() is defered otherwise it will lead to loosing a decrement if some error occurs.
	var (
		timeseries = r.Timeseries
		metadata   = r.Metadata
	)
	release := func() { FinishWriteRequest(r) }
	switch numTs, numMeta := len(timeseries), len(metadata); {
	case numTs > 0 && numMeta == 0:
		// Write request contains only time-series.
		n, err := ingestor.ingestTimeseries(timeseries, release)
		return n, 0, err
	case numTs == 0 && numMeta == 0:
		release()
		return 0, 0, nil
	case numMeta > 0 && numTs == 0:
		// Write request contains only metadata.
		n, err := ingestor.ingestMetadata(metadata, release)
		return 0, n, err
	default:
	}
	release = func() {
		// We do not want to re-initialize the write-request when ingesting concurrently.
		// A concurrent ingestion may not have read the write-request and we initialized it
		// leading to data loss.
		FinishWriteRequest(nil)
	}
	res := make(chan result, 2)
	defer close(res)

	go func() {
		n, err := ingestor.ingestTimeseries(timeseries, release)
		res <- result{series, n, err}
	}()
	go func() {
		n, err := ingestor.ingestMetadata(metadata, release)
		res <- result{meta, n, err}
	}()

	mergeErr := func(prevErr, err error, message string) error {
		if prevErr != nil {
			err = fmt.Errorf("%s: %s: %w", prevErr.Error(), message, err)
		}
		return err
	}

	for i := 0; i < 2; i++ {
		response := <-res
		switch response.id {
		case series:
			numInsertablesIngested = response.numRows
			err = mergeErr(err, response.err, "ingesting timeseries")
		case meta:
			numMetadataIngested = response.numRows
			err = mergeErr(err, response.err, "ingesting metadata")
		}
	}
	// WriteRequests can contain pointers into the original buffer we deserialized
	// them out of, and can be quite large in and of themselves. In order to prevent
	// memory blowup, and to allow faster deserializing, we recycle the WriteRequest
	// here, allowing it to be either garbage collected or reused for a new request.
	// In order for this to work correctly, any data we wish to keep using (e.g.
	// samples) must no longer be reachable from req.
	FinishWriteRequest(r)
	return numInsertablesIngested, numMetadataIngested, err
}

func (ingestor *DBIngestor) ingestTimeseries(timeseries []prompb.TimeSeries, releaseMem func()) (uint64, error) {
	var (
		totalRowsExpected uint64

		insertables = make(map[string][]model.Insertable)
	)

	for i := range timeseries {
		var (
			err        error
			series     *model.Series
			metricName string

			ts = &timeseries[i]
		)
		if len(ts.Labels) == 0 {
			continue
		}
		// Normalize and canonicalize t.Labels.
		// After this point t.Labels should never be used again.
		series, metricName, err = ingestor.sCache.GetSeriesFromProtos(ts.Labels)
		if err != nil {
			return 0, err
		}
		if metricName == "" {
			return 0, errors.ErrNoMetricName
		}

		if len(ts.Samples) > 0 {
			samples, count, err := ingestor.samples(series, ts)
			if err != nil {
				return 0, fmt.Errorf("samples: %w", err)
			}
			totalRowsExpected += uint64(count)
			insertables[metricName] = append(insertables[metricName], samples)
		}
		if len(ts.Exemplars) > 0 {
			exemplars, count, err := ingestor.exemplars(series, ts)
			if err != nil {
				return 0, fmt.Errorf("exemplars: %w", err)
			}
			totalRowsExpected += uint64(count)
			insertables[metricName] = append(insertables[metricName], exemplars)
		}
		// we're going to free req after this, but we still need the samples,
		// so nil the field
		ts.Samples = nil
		ts.Exemplars = nil
	}
	releaseMem()

	numInsertablesIngested, errSamples := ingestor.dispatcher.InsertTs(model.Data{Rows: insertables, ReceivedTime: time.Now()})
	if errSamples == nil && numInsertablesIngested != totalRowsExpected {
		return numInsertablesIngested, fmt.Errorf("failed to insert all the data! Expected: %d, Got: %d", totalRowsExpected, numInsertablesIngested)
	}
	return numInsertablesIngested, errSamples
}

func (ingestor *DBIngestor) samples(l *model.Series, ts *prompb.TimeSeries) (model.Insertable, int, error) {
	return model.NewPromSamples(l, ts.Samples), len(ts.Samples), nil
}

func (ingestor *DBIngestor) exemplars(l *model.Series, ts *prompb.TimeSeries) (model.Insertable, int, error) {
	return model.NewPromExemplars(l, ts.Exemplars), len(ts.Exemplars), nil
}

// ingestMetadata ingests metric metadata received from Prometheus. It runs as a secondary routine, independent from
// the main dataflow (i.e., samples ingestion) since metadata ingestion is not as frequent as that of samples.
func (ingestor *DBIngestor) ingestMetadata(metadata []prompb.MetricMetadata, releaseMem func()) (uint64, error) {
	num := len(metadata)
	data := make([]model.Metadata, num)
	for i := 0; i < num; i++ {
		tmp := metadata[i]
		data[i] = model.Metadata{
			MetricFamily: tmp.MetricFamilyName,
			Unit:         tmp.Unit,
			Type:         tmp.Type.String(),
			Help:         tmp.Help,
		}
	}
	releaseMem()
	numMetadataIngested, errMetadata := ingestor.dispatcher.InsertMetadata(data)
	if errMetadata != nil {
		return 0, errMetadata
	}
	return numMetadataIngested, nil
}

// Parts of metric creation not needed to insert data
func (ingestor *DBIngestor) CompleteMetricCreation() error {
	return ingestor.dispatcher.CompleteMetricCreation()
}

// Close closes the ingestor
func (ingestor *DBIngestor) Close() {
	ingestor.dispatcher.Close()
}
