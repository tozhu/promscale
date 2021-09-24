// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgtype"
	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/common/schema"
	"github.com/timescale/promscale/pkg/pgmodel/metrics"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	tput "github.com/timescale/promscale/pkg/util/throughput"
	"go.opentelemetry.io/collector/model/pdata"
)

type TagType uint

const (
	MetricBatcherChannelCap = 1000
	finalizeMetricCreation  = "CALL " + schema.Catalog + ".finalize_metric_creation()"
	getEpochSQL             = "SELECT current_epoch FROM " + schema.Catalog + ".ids_epoch LIMIT 1"

	SpanTagType TagType = 1 << iota
	ResourceTagType
	EventTagType
	LinkTagType
)

// pgxDispatcher redirects incoming samples to the appropriate metricBatcher
// corresponding to the metric in the sample.
type pgxDispatcher struct {
	conn                   pgxconn.PgxConn
	metricTableNames       cache.MetricCache
	scache                 cache.SeriesCache
	exemplarKeyPosCache    cache.PositionCache
	batchers               sync.Map
	completeMetricCreation chan struct{}
	asyncAcks              bool
	copierReadRequestCh    chan<- readRequest
	seriesEpochRefresh     *time.Ticker
	doneChannel            chan struct{}
	doneWG                 sync.WaitGroup
	labelArrayOID          uint32
}

func newPgxDispatcher(conn pgxconn.PgxConn, cache cache.MetricCache, scache cache.SeriesCache, eCache cache.PositionCache, cfg *Cfg) (*pgxDispatcher, error) {
	numCopiers := cfg.NumCopiers
	if numCopiers < 1 {
		log.Warn("msg", "num copiers less than 1, setting to 1")
		numCopiers = 1
	}

	//the copier read request channel keep the queue order
	//between metrucs
	maxMetrics := 10000
	copierReadRequestCh := make(chan readRequest, maxMetrics)
	setCopierChannelToMonitor(copierReadRequestCh)

	if cfg.IgnoreCompressedChunks {
		// Handle decompression to not decompress anything.
		handleDecompression = skipDecompression
	}

	if err := model.RegisterCustomPgTypes(conn); err != nil {
		return nil, fmt.Errorf("registering custom pg types: %w", err)
	}

	labelArrayOID := model.GetCustomTypeOID(model.LabelArray)
	sw := NewSeriesWriter(conn, labelArrayOID)
	elf := NewExamplarLabelFormatter(conn, eCache)

	for i := 0; i < numCopiers; i++ {
		go runCopier(conn, copierReadRequestCh, sw, elf)
	}

	inserter := &pgxDispatcher{
		conn:                   conn,
		metricTableNames:       cache,
		scache:                 scache,
		exemplarKeyPosCache:    eCache,
		completeMetricCreation: make(chan struct{}, 1),
		asyncAcks:              cfg.AsyncAcks,
		copierReadRequestCh:    copierReadRequestCh,
		// set to run at half our deletion interval
		seriesEpochRefresh: time.NewTicker(30 * time.Minute),
		doneChannel:        make(chan struct{}),
	}
	runBatchWatcher(inserter.doneChannel)

	//on startup run a completeMetricCreation to recover any potentially
	//incomplete metric
	if err := inserter.CompleteMetricCreation(); err != nil {
		return nil, err
	}

	go inserter.runCompleteMetricCreationWorker()

	if !cfg.DisableEpochSync {
		inserter.doneWG.Add(1)
		go func() {
			defer inserter.doneWG.Done()
			inserter.runSeriesEpochSync()
		}()
	}
	return inserter, nil
}

func (p *pgxDispatcher) runCompleteMetricCreationWorker() {
	for range p.completeMetricCreation {
		err := p.CompleteMetricCreation()
		if err != nil {
			log.Warn("msg", "Got an error finalizing metric", "err", err)
		}
	}
}

func (p *pgxDispatcher) runSeriesEpochSync() {
	epoch, err := p.refreshSeriesEpoch(model.InvalidSeriesEpoch)
	// we don't have any great place to report errors, and if the
	// connection recovers we can still make progress, so we'll just log it
	// and continue execution
	if err != nil {
		log.Error("msg", "error refreshing the series cache", "err", err)
	}
	for {
		select {
		case <-p.seriesEpochRefresh.C:
			epoch, err = p.refreshSeriesEpoch(epoch)
			if err != nil {
				log.Error("msg", "error refreshing the series cache", "err", err)
			}
		case <-p.doneChannel:
			return
		}
	}
}

func (p *pgxDispatcher) refreshSeriesEpoch(existingEpoch model.SeriesEpoch) (model.SeriesEpoch, error) {
	dbEpoch, err := p.getServerEpoch()
	if err != nil {
		// Trash the cache just in case an epoch change occurred, seems safer
		p.scache.Reset()
		return model.InvalidSeriesEpoch, err
	}
	if existingEpoch == model.InvalidSeriesEpoch || dbEpoch != existingEpoch {
		p.scache.Reset()
	}
	return dbEpoch, nil
}

func (p *pgxDispatcher) getServerEpoch() (model.SeriesEpoch, error) {
	var newEpoch int64
	row := p.conn.QueryRow(context.Background(), getEpochSQL)
	err := row.Scan(&newEpoch)
	if err != nil {
		return -1, err
	}

	return model.SeriesEpoch(newEpoch), nil
}

func (p *pgxDispatcher) CompleteMetricCreation() error {
	_, err := p.conn.Exec(
		context.Background(),
		finalizeMetricCreation,
	)
	return err
}

func (p *pgxDispatcher) Close() {
	close(p.completeMetricCreation)
	p.batchers.Range(func(key, value interface{}) bool {
		close(value.(chan *insertDataRequest))
		return true
	})
	close(p.copierReadRequestCh)
	close(p.doneChannel)
	p.doneWG.Wait()
}

func (p *pgxDispatcher) InsertSchemaURL(ctx context.Context, sURL string) (id pgtype.Int8, err error) {
	if sURL == "" {
		id.Status = pgtype.Null
		return id, nil
	}
	err = p.conn.QueryRow(ctx, "INSERT INTO "+schema.Trace+".schema_url (url) VALUES ( $1 ) RETURNING (id)", sURL).Scan(&id)
	return id, err
}

func (p *pgxDispatcher) InsertSpanName(ctx context.Context, name string) (id pgtype.Int8, err error) {
	if name == "" {
		id.Status = pgtype.Null
		return id, nil
	}
	err = p.conn.QueryRow(ctx, "INSERT INTO "+schema.Trace+".span_name (name) VALUES ( $1 ) ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name RETURNING (id)", name).Scan(&id)
	return id, err
}

func (p *pgxDispatcher) InsertInstrumentationLibrary(ctx context.Context, name, version, schemaURL string) (id pgtype.Int8, err error) {
	if name == "" || version == "" {
		id.Status = pgtype.Null
		return id, nil
	}
	var sID pgtype.Int8
	if schemaURL != "" {
		schemaURLID, err := p.InsertSchemaURL(ctx, schemaURL)
		if err != nil {
			return id, err
		}
		err = sID.Set(schemaURLID)
		if err != nil {
			return id, err
		}
	} else {
		sID.Status = pgtype.Null
	}

	err = p.conn.QueryRow(ctx, "INSERT INTO "+schema.Trace+".instrumentation_lib (name, version, schema_url_id) VALUES ( $1, $2, $3 ) RETURNING (id)", name, version, sID).Scan(&id)
	return id, err
}

func (p *pgxDispatcher) insertTags(ctx context.Context, tags map[string]interface{}, typ TagType) error {
	for k, v := range tags {
		_, err := p.conn.Exec(ctx, "SELECT "+schema.TracePublic+".put_tag_key($1, $2::"+schema.TracePublic+".tag_type)", k, typ)

		if err != nil {
			return err
		}

		val, err := json.Marshal(v)
		if err != nil {
			return err
		}

		_, err = p.conn.Exec(ctx, "SELECT "+schema.TracePublic+".put_tag($1, $2, $3::"+schema.TracePublic+".tag_type)",
			k,
			string(val),
			typ,
		)

		if err != nil {
			return err
		}
	}

	return nil
}

func (p *pgxDispatcher) InsertSpanLinks(ctx context.Context, links pdata.SpanLinkSlice, traceID [16]byte, spanID [8]byte, spanStartTime time.Time) error {
	spanIDInt := convertByteArrayToInt64(spanID)
	for i := 0; i < links.Len(); i++ {
		link := links.At(i)
		linkedSpanIDInt := convertByteArrayToInt64(link.SpanID().Bytes())

		rawTags := link.Attributes().AsRaw()
		if err := p.insertTags(ctx, rawTags, LinkTagType); err != nil {
			return err
		}
		jsonTags, err := json.Marshal(rawTags)
		if err != nil {
			return err
		}
		_, err = p.conn.Exec(ctx, "INSERT INTO "+schema.Trace+".link (trace_id, span_id, span_start_time, linked_trace_id, linked_span_id, trace_state, tags, dropped_tags_count, link_nbr) VALUES ( $1, $2, $3, $4, $5, $6, $7, $8, $9 )",
			getUUID(traceID),
			spanIDInt,
			spanStartTime,
			getUUID(link.TraceID().Bytes()),
			linkedSpanIDInt,
			link.TraceState(),
			string(jsonTags),
			link.DroppedAttributesCount(),
			i,
		)

		if err != nil {
			return err
		}
	}
	return nil
}

func (p *pgxDispatcher) InsertSpanEvents(ctx context.Context, events pdata.SpanEventSlice, traceID [16]byte, spanID [8]byte) error {
	spanIDInt := convertByteArrayToInt64(spanID)
	for i := 0; i < events.Len(); i++ {
		event := events.At(i)
		rawTags := event.Attributes().AsRaw()
		if err := p.insertTags(ctx, rawTags, EventTagType); err != nil {
			return err
		}
		jsonTags, err := json.Marshal(rawTags)
		if err != nil {
			return err
		}
		_, err = p.conn.Exec(ctx, "INSERT INTO "+schema.Trace+".event (time, trace_id, span_id, name, event_nbr, tags, dropped_tags_count) VALUES ( $1, $2, $3, $4, $5, $6, $7 )",
			event.Timestamp().AsTime(),
			getUUID(traceID),
			spanIDInt,
			event.Name(),
			i,
			string(jsonTags),
			event.DroppedAttributesCount(),
		)

		if err != nil {
			return err
		}
	}
	return nil
}

func convertByteArrayToInt64(buf [8]byte) int64 {
	ux := binary.BigEndian.Uint64(buf[:])

	x := int64(ux >> 1)
	if ux&1 != 0 {
		x = ^x
	}

	return x
}

func getUUID(buf [16]byte) pgtype.UUID {
	return pgtype.UUID{
		Bytes:  buf,
		Status: pgtype.Present,
	}
}

func getEventTimeRange(events pdata.SpanEventSlice) (result pgtype.Tstzrange) {
	if events.Len() == 0 {
		_ = result.Set(nil)
		return result
	}

	var lowerTime, upperTime time.Time

	for i := 0; i < events.Len(); i++ {
		eventTime := events.At(i).Timestamp().AsTime()

		if lowerTime.IsZero() || eventTime.Before(lowerTime) {
			lowerTime = eventTime
		}
		if upperTime.IsZero() || eventTime.After(upperTime) {
			upperTime = eventTime
		}
	}

	result = pgtype.Tstzrange{
		Lower:     pgtype.Timestamptz{Time: lowerTime, Status: pgtype.Present},
		Upper:     pgtype.Timestamptz{Time: upperTime, Status: pgtype.Present},
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Exclusive,
		Status:    pgtype.Present,
	}

	return result
}

func (p *pgxDispatcher) InsertSpan(ctx context.Context, span pdata.Span, nameID, instLibID, rSchemaURLID pgtype.Int8, resourceTags pdata.AttributeMap) error {
	spanIDInt := convertByteArrayToInt64(span.SpanID().Bytes())
	parentSpanIDInt := convertByteArrayToInt64(span.ParentSpanID().Bytes())
	rawResourceTags := resourceTags.AsRaw()
	if err := p.insertTags(ctx, rawResourceTags, ResourceTagType); err != nil {
		return err
	}
	jsonResourceTags, err := json.Marshal(rawResourceTags)
	if err != nil {
		return err
	}
	rawTags := span.Attributes().AsRaw()
	if err := p.insertTags(ctx, rawTags, SpanTagType); err != nil {
		return err
	}
	jsonTags, err := json.Marshal(rawTags)
	if err != nil {
		return err
	}

	eventTimeRange := getEventTimeRange(span.Events())

	_, err = p.conn.Exec(
		ctx,
		"INSERT INTO "+schema.Trace+`.span 
			(trace_id, span_id, trace_state, parent_span_id, name_id, 
			span_kind, start_time, end_time, span_tags, dropped_tags_count,
			event_time, dropped_events_count, dropped_link_count, status_code,
			status_message, instrumentation_lib_id, resource_tags, resource_dropped_tags_count,
			resource_schema_url_id) 
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)`,
		getUUID(span.TraceID().Bytes()),
		spanIDInt,
		span.TraceState(),
		parentSpanIDInt,
		nameID,
		span.Kind().String(),
		span.StartTimestamp().AsTime(),
		span.EndTimestamp().AsTime(),
		string(jsonTags),
		span.DroppedAttributesCount(),
		eventTimeRange,
		span.DroppedEventsCount(),
		span.DroppedLinksCount(),
		span.Status().Code().String(),
		span.Status().Message(),
		instLibID,
		string(jsonResourceTags),
		0, // TODO: Add resource_dropped_tags_count when it gets exposed upstream.
		rSchemaURLID,
	)

	return err
}

// InsertTs inserts a batch of data into the database.
// The data should be grouped by metric name.
// returns the number of rows we intended to insert (_not_ how many were
// actually inserted) and any error.
// Though we may insert data to multiple tables concurrently, if asyncAcks is
// unset this function will wait until _all_ the insert attempts have completed.
func (p *pgxDispatcher) InsertTs(dataTS model.Data) (uint64, error) {
	var (
		numRows      uint64
		maxt         int64
		rows         = dataTS.Rows
		workFinished = new(sync.WaitGroup)
	)
	workFinished.Add(len(rows))
	// we only allocate enough space for a single error message here as we only
	// report one error back upstream. The inserter should not block on this
	// channel, but only insert if it's empty, anything else can deadlock.
	errChan := make(chan error, 1)
	for metricName, data := range rows {
		for _, insertable := range data {
			numRows += uint64(insertable.Count())
			ts := insertable.MaxTs()
			if maxt < ts {
				maxt = ts
			}
		}
		// the following is usually non-blocking, just a channel insert
		p.getMetricBatcher(metricName) <- &insertDataRequest{metric: metricName, data: data, finished: workFinished, errChan: errChan}
	}
	reportIncomingBatch(numRows)
	reportOutgoing := func() {
		reportOutgoingBatch(numRows)
		reportBatchProcessingTime(dataTS.ReceivedTime)
	}

	var err error
	if !p.asyncAcks {
		workFinished.Wait()
		reportOutgoing()
		select {
		case err = <-errChan:
		default:
		}
		postIngestTasks(maxt, numRows, 0)
		close(errChan)
	} else {
		go func() {
			workFinished.Wait()
			reportOutgoing()
			select {
			case err = <-errChan:
			default:
			}
			close(errChan)
			if err != nil {
				log.Error("msg", fmt.Sprintf("error on async send, dropping %d datapoints", numRows), "err", err)
			}
			postIngestTasks(maxt, numRows, 0)
		}()
	}

	return numRows, err
}

func (p *pgxDispatcher) InsertMetadata(metadata []model.Metadata) (uint64, error) {
	totalRows := uint64(len(metadata))
	insertedRows, err := insertMetadata(p.conn, metadata)
	if err != nil {
		return insertedRows, err
	}
	postIngestTasks(0, 0, insertedRows)
	if totalRows != insertedRows {
		return insertedRows, fmt.Errorf("failed to insert all metadata: inserted %d rows out of %d rows in total", insertedRows, totalRows)
	}
	return insertedRows, nil
}

// postIngestTasks performs a set of tasks that are due after ingesting series data.
func postIngestTasks(maxTs int64, numSamples, numMetadata uint64) {
	tput.ReportDataProcessed(maxTs, numSamples, numMetadata)

	// Max_sent_timestamp stats.
	if maxTs < atomic.LoadInt64(&MaxSentTimestamp) {
		return
	}
	atomic.StoreInt64(&MaxSentTimestamp, maxTs)
	metrics.MaxSentTimestamp.Set(float64(maxTs))
}

// Get the handler for a given metric name, creating a new one if none exists
func (p *pgxDispatcher) getMetricBatcher(metric string) chan<- *insertDataRequest {
	batcher, ok := p.batchers.Load(metric)
	if !ok {
		// The ordering is important here: we need to ensure that every call
		// to getMetricInserter() returns the same inserter. Therefore, we can
		// only start up the inserter routine if we know that we won the race
		// to create the inserter, anything else will leave a zombie inserter
		// lying around.
		c := make(chan *insertDataRequest, MetricBatcherChannelCap)
		actual, old := p.batchers.LoadOrStore(metric, c)
		batcher = actual
		if !old {
			go runMetricBatcher(p.conn, c, metric, p.completeMetricCreation, p.metricTableNames, p.copierReadRequestCh, p.labelArrayOID)
		}
	}
	ch := batcher.(chan *insertDataRequest)
	MetricBatcherChLen.Observe(float64(len(ch)))
	return ch
}

type insertDataRequest struct {
	metric   string
	finished *sync.WaitGroup
	data     []model.Insertable
	errChan  chan error
}

func (idr *insertDataRequest) reportResult(err error) {
	if err != nil {
		select {
		case idr.errChan <- err:
		default:
		}
	}
	idr.finished.Done()
}
