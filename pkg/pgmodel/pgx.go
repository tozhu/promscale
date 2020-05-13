// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package pgmodel

import (
	"container/list"
	"context"
	"fmt"
	"hash/maphash"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/allegro/bigcache"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

const (
	promSchema       = "prom_api"
	seriesViewSchema = "prom_series"
	metricViewSchema = "prom_metric"
	dataSchema       = "prom_data"
	dataSeriesSchema = "prom_data_series"
	infoSchema       = "prom_info"
	catalogSchema    = "_prom_catalog"
	extSchema        = "_prom_ext"

	getMetricsTableSQL       = "SELECT table_name FROM " + catalogSchema + ".get_metric_table_name_if_exists($1)"
	getCreateMetricsTableSQL = "SELECT table_name FROM " + catalogSchema + ".get_or_create_metric_table_name($1)"
	getSeriesIDForLabelSQL   = "SELECT * FROM " + catalogSchema + ".get_series_id_for_key_value_array($1, $2, $3)"
)

var (
	copyColumns         = []string{"time", "value", "series_id"}
	errMissingTableName = fmt.Errorf("missing metric table name")
)

type pgxBatch interface {
	Queue(query string, arguments ...interface{})
}

type pgxConn interface {
	Close()
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
	CopyFromRows(rows [][]interface{}) pgx.CopyFromSource
	NewBatch() pgxBatch
	SendBatch(ctx context.Context, b pgxBatch) (pgx.BatchResults, error)
}

// MetricCache provides a caching mechanism for metric table names.
type MetricCache interface {
	Get(metric string) (string, error)
	Set(metric string, tableName string) error
}

type pgxConnImpl struct {
	conn *pgxpool.Pool
}

func (p *pgxConnImpl) getConn() *pgxpool.Pool {
	return p.conn
}

func (p *pgxConnImpl) Close() {
	conn := p.getConn()
	p.conn = nil
	conn.Close()
}

func (p *pgxConnImpl) Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error) {
	conn := p.getConn()

	return conn.Exec(ctx, sql, arguments...)
}

func (p *pgxConnImpl) Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	conn := p.getConn()

	return conn.Query(ctx, sql, args...)
}

func (p *pgxConnImpl) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	conn := p.getConn()

	return conn.CopyFrom(ctx, tableName, columnNames, rowSrc)
}

func (p *pgxConnImpl) CopyFromRows(rows [][]interface{}) pgx.CopyFromSource {
	return pgx.CopyFromRows(rows)
}

func (p *pgxConnImpl) NewBatch() pgxBatch {
	return &pgx.Batch{}
}

func (p *pgxConnImpl) SendBatch(ctx context.Context, b pgxBatch) (pgx.BatchResults, error) {
	conn := p.getConn()

	return conn.SendBatch(ctx, b.(*pgx.Batch)), nil
}

// SampleInfoIterator is an iterator over a collection of sampleInfos that returns
// data in the format expected for the data table row.
type SampleInfoIterator struct {
	sampleInfos     []labeledSamplesInfo
	sampleInfoIndex int
	sampleIndex     int
}

// NewSampleInfoIterator is the constructor
func NewSampleInfoIterator() *SampleInfoIterator {
	return &SampleInfoIterator{sampleInfos: make([]labeledSamplesInfo, 0), sampleIndex: -1, sampleInfoIndex: 0}
}

//Append adds a sample info to the back of the iterator
func (t *SampleInfoIterator) Append(s labeledSamplesInfo) {
	t.sampleInfos = append(t.sampleInfos, s)
}

// Next returns true if there is another row and makes the next row data
// available to Values(). When there are no more rows available or an error
// has occurred it returns false.
func (t *SampleInfoIterator) Next() bool {
	t.sampleIndex++
	if t.sampleInfoIndex < len(t.sampleInfos) && t.sampleIndex >= len(t.sampleInfos[t.sampleInfoIndex].samples) {
		t.sampleInfoIndex++
		t.sampleIndex = 0
	}
	return t.sampleInfoIndex < len(t.sampleInfos)
}

// Values returns the values for the current row
func (t *SampleInfoIterator) Values() ([]interface{}, error) {
	info := t.sampleInfos[t.sampleInfoIndex]
	sample := info.samples[t.sampleIndex]
	row := []interface{}{
		model.Time(sample.Timestamp).Time(),
		sample.Value,
		info.seriesID,
	}
	return row, nil
}

// Err returns any error that has been encountered by the CopyFromSource. If
// this is not nil *Conn.CopyFrom will abort the copy.
func (t *SampleInfoIterator) Err() error {
	return nil
}

// NewPgxIngestorWithMetricCache returns a new Ingestor that uses connection pool and a metrics cache
// for caching metric table names.
func NewPgxIngestorWithMetricCache(c *pgxpool.Pool, cache MetricCache) *DBIngestor {

	conn := &pgxConnImpl{
		conn: c,
	}

	pi := newPgxInserter(conn, cache)

	series, _ := bigcache.NewBigCache(DefaultCacheConfig())

	bc := &bCache{
		series: series,
	}

	return &DBIngestor{
		db:    pi,
		cache: bc,
	}
}

// NewPgxIngestor returns a new Ingestor that write to PostgreSQL using PGX
func NewPgxIngestor(c *pgxpool.Pool) *DBIngestor {
	metrics, _ := bigcache.NewBigCache(DefaultCacheConfig())
	cache := &MetricNameCache{metrics}
	return NewPgxIngestorWithMetricCache(c, cache)
}

func newPgxInserter(conn pgxConn, cache MetricCache) *pgxInserter {
	maxProcs := runtime.GOMAXPROCS(-1)
	if maxProcs <= 0 {
		maxProcs = runtime.NumCPU()
	}
	if maxProcs <= 0 {
		maxProcs = 1
	}
	inserters := make([]chan insertDataRequest, maxProcs)
	for i := 0; i < maxProcs; i++ {
		ch := make(chan insertDataRequest, 1000)
		inserters[i] = ch
		go runInserterRoutine(conn, ch)
	}

	return &pgxInserter{
		conn:             conn,
		metricTableNames: cache,
		inserters:        inserters,
		seed:             maphash.MakeSeed(),
	}
}

type pgxInserter struct {
	conn             pgxConn
	metricTableNames MetricCache
	inserters        []chan insertDataRequest
	seed             maphash.Seed
}

func (p *pgxInserter) Close() {
	for i := 0; i < len(p.inserters); i++ {
		close(p.inserters[i])
	}
}

func (p *pgxInserter) InsertNewData(rows map[string][]labeledSamplesInfo) (uint64, error) {
	return p.InsertData(rows)
}

type insertDataRequest struct {
	metric   string
	data     []labeledSamplesInfo
	finished *sync.WaitGroup
	errChan  chan error
}

type insertDataTask struct {
	finished *sync.WaitGroup
	errChan  chan error
}

func (p *pgxInserter) InsertData(rows map[string][]labeledSamplesInfo) (uint64, error) {
	var numRows uint64
	workFinished := &sync.WaitGroup{}
	workFinished.Add(len(rows))
	errChan := make(chan error, 1)
	for metricName, data := range rows {
		for _, si := range data {
			numRows += uint64(len(si.samples))
		}
		p.insertMetricData(metricName, data, workFinished, errChan)
	}

	workFinished.Wait()
	var err error
	select {
	case err = <-errChan:
	default:
	}

	return numRows, err
}

func (p *pgxInserter) insertMetricData(metric string, data []labeledSamplesInfo, finished *sync.WaitGroup, errChan chan error) {
	inserter := p.getMetricInserter(metric)
	inserter <- insertDataRequest{metric: metric, data: data, finished: finished, errChan: errChan}
}

func (p *pgxInserter) createMetricTable(metric string) (string, error) {
	res, err := p.conn.Query(
		context.Background(),
		getCreateMetricsTableSQL,
		metric,
	)

	if err != nil {
		return "", err
	}

	var tableName string
	defer res.Close()
	if !res.Next() {
		return "", errMissingTableName
	}

	if err := res.Scan(&tableName); err != nil {
		return "", err
	}

	return tableName, nil
}

func (p *pgxInserter) getMetricTableName(metric string) (string, error) {
	var err error
	var tableName string

	tableName, err = p.metricTableNames.Get(metric)

	if err == nil {
		return tableName, nil
	}

	if err != ErrEntryNotFound {
		return "", err
	}

	tableName, err = p.createMetricTable(metric)

	if err != nil {
		return "", err
	}

	err = p.metricTableNames.Set(metric, tableName)

	return tableName, err
}

func (p *pgxInserter) getMetricInserter(metric string) chan insertDataRequest {
	h := maphash.Hash{}
	h.SetSeed(p.seed)
	_, err := h.WriteString(metric)
	if err != nil {
		panic(fmt.Sprintf("error hashing metric table name: %v", err))
	}
	if len(p.inserters) < 1 {
		panic(fmt.Sprintf("invalid len %d", len(p.inserters)))
	}
	inserter := h.Sum64() % uint64(len(p.inserters))
	return p.inserters[inserter]
}

type insertHandler struct {
	conn    pgxConn
	input   chan insertDataRequest
	pending orderedMap

	seriesCache      sync.Map // map[string]SeriesID
	metricTableNames sync.Map // map[string]string

	toSeriesInserter chan seriesInsertRequest
	toCopier         chan seriesInsertResponse
}

type orderedMap struct {
	elements map[string]*list.Element
	order    list.List // list of PendingBuffer

	oldBuffers chan *pendingBuffer
}

type pendingBuffer struct {
	metric        string
	needsResponse []insertDataTask
	batch         SampleInfoIterator
	start         time.Time
}

type seriesInsertRequest struct {
	buffer           *pendingBuffer
	numMissingSeries int
}

type seriesInsertResponse struct {
	buffer *pendingBuffer
	err    error
}

const (
	flushSize    = 2000
	flushTimeout = 500 * time.Millisecond
)

func runInserterRoutine(conn pgxConn, input chan insertDataRequest) {
	seriesInsertChan := make(chan seriesInsertRequest, 1000)
	toCopier := make(chan seriesInsertResponse, 1000)
	handler := &insertHandler{
		conn:             conn,
		input:            input,
		pending:          makeOrderedMap(),
		toSeriesInserter: seriesInsertChan,
		toCopier:         toCopier,
	}
	go handler.runSeriesInserterRoutine(seriesInsertChan, toCopier)
	go handler.runCopy(toCopier)

	for {
		if !handler.hasPendingReqs() {
			stillAlive := handler.blockingHandleReq()
			if !stillAlive {
				return
			}
			continue
		}

		startHandling := time.Now()
	hotReceive:
		for {
			for i := 0; i < 1000; i++ {
				receivingNewReqs := handler.nonblockingHandleReq()
				if !receivingNewReqs {
					break hotReceive
				}
			}
			if time.Since(startHandling) > flushTimeout {
				handler.flushTimedOutReqs()
				startHandling = time.Now()
			}
		}

		handler.flushEarliestReq()
	}
}

func (h *insertHandler) hasPendingReqs() bool {
	return !h.pending.IsEmpty()
}

func (h *insertHandler) blockingHandleReq() bool {
	req, ok := <-h.input
	if !ok {
		return false
	}

	h.handleReq(req)

	return true
}

func (h *insertHandler) nonblockingHandleReq() bool {
	select {
	case req := <-h.input:
		h.handleReq(req)
		return true
	default:
		return false
	}
}

func (h *insertHandler) handleReq(req insertDataRequest) bool {
	needsFlush, pending := h.pending.addReq(req)
	if needsFlush {
		h.flushPending(pending)
		return true
	}
	return false
}

func (h *insertHandler) flushTimedOutReqs() {
	for {
		earliest, earliestPending := h.pending.Front()
		if earliest == nil {
			return
		}

		elapsed := time.Since(earliestPending.start)
		if elapsed < flushTimeout {
			return
		}

		h.flushPending(earliest)
	}
}

func (h *insertHandler) flushEarliestReq() {
	earliest, _ := h.pending.Front()
	if earliest == nil {
		return
	}

	h.flushPending(earliest)
}

func (h *insertHandler) flushPending(pendingElem *list.Element) {
	pending := h.pending.Remove(pendingElem)

	ok := h.setSeriesIds(pending)
	if !ok {
		return
	}

	h.toCopier <- seriesInsertResponse{pending, nil}
}

func (h *insertHandler) runCopy(input chan seriesInsertResponse) {
	for {
		req, ok := <-input
		if !ok {
			return
		}

		if req.err != nil {
			h.ackInsertDataRequest(req.buffer, req.err)
			continue
		}

		err := h.sendSeriesData(req.buffer)
		h.ackInsertDataRequest(req.buffer, err)
	}
}

func (h *insertHandler) sendSeriesData(pending *pendingBuffer) error {
	tableName, _ := h.metricTableNames.Load(pending.metric)
	_, err := h.conn.CopyFrom(
		context.Background(),
		pgx.Identifier{dataSchema, tableName.(string)},
		copyColumns,
		&pending.batch,
	)
	return err
}

func (h *insertHandler) ackInsertDataRequest(pending *pendingBuffer, err error) {
	for i := 0; i < len(pending.needsResponse); i++ {
		if err != nil {
			select {
			case pending.needsResponse[i].errChan <- err:
			default:
			}
		}
		pending.needsResponse[i].finished.Done()
		pending.needsResponse[i] = insertDataTask{}
	}
	pending.needsResponse = pending.needsResponse[:0]

	for i := 0; i < len(pending.batch.sampleInfos); i++ {
		// nil all pointers to prevent memory leaks
		pending.batch.sampleInfos[i] = labeledSamplesInfo{}
	}
	pending.batch = SampleInfoIterator{sampleInfos: pending.batch.sampleInfos[:0], sampleIndex: -1, sampleInfoIndex: 0}

	h.pending.giveOldBuffer(pending)
}

func (h *insertHandler) runSeriesInserterRoutine(input chan seriesInsertRequest, output chan seriesInsertResponse) {
	for {
		req, ok := <-input
		if !ok {
			return
		}

		buffer := req.buffer
		seriesToInsert := make([]*labeledSamplesInfo, 0, req.numMissingSeries)
		for i, series := range buffer.batch.sampleInfos {
			if series.seriesID < 0 {
				seriesToInsert = append(seriesToInsert, &buffer.batch.sampleInfos[i])
			}
		}
		var lastSeenLabel Labels

		batch := h.conn.NewBatch()
		numSQLFunctionCalls := 0
		// Sort and remove duplicates. The sort is needed to remove duplicates. Each series is inserted
		// in a different transaction, thus deadlocks are not an issue.
		sort.Slice(seriesToInsert, func(i, j int) bool {
			return seriesToInsert[i].labels.Compare(seriesToInsert[j].labels) < 0
		})

		batchSeries := make([][]*labeledSamplesInfo, 0, len(seriesToInsert))
		// group the seriesToInsert by labels, one slice array per unique labels
		var metric string
		for _, curr := range seriesToInsert {
			if !lastSeenLabel.isEmpty() && lastSeenLabel.Equal(curr.labels) {
				batchSeries[len(batchSeries)-1] = append(batchSeries[len(batchSeries)-1], curr)
				continue
			}

			if metric == "" {
				metric = curr.labels.metricName
			}

			batch.Queue("BEGIN;")
			batch.Queue(getSeriesIDForLabelSQL, curr.labels.metricName, curr.labels.names, curr.labels.values)
			batch.Queue("COMMIT;")
			numSQLFunctionCalls++
			batchSeries = append(batchSeries, []*labeledSamplesInfo{curr})

			lastSeenLabel = curr.labels
		}

		br, err := h.conn.SendBatch(context.Background(), batch)

		err = func() error {
			if err != nil {
				return err
			}
			defer br.Close()

			if numSQLFunctionCalls != len(batchSeries) {
				return fmt.Errorf("unexpected difference in numQueries and batchSeries")
			}

			var tableName string
			for i := 0; i < numSQLFunctionCalls; i++ {
				_, err = br.Exec()
				if err != nil {
					return err
				}
				row := br.QueryRow()

				var id SeriesID
				err = row.Scan(&tableName, &id)
				if err != nil {
					return err
				}
				h.seriesCache.Store(batchSeries[i][0].labels.String(), id)
				for _, lsi := range batchSeries[i] {
					lsi.seriesID = id
				}
				_, err = br.Exec()
				if err != nil {
					return err
				}
			}
			h.metricTableNames.Store(metric, tableName)
			return nil
		}()

		output <- seriesInsertResponse{buffer, err}
	}
}

func (h *insertHandler) setSeriesIds(buffer *pendingBuffer) bool {
	numMissingSeries := 0

	for i, series := range buffer.batch.sampleInfos {
		id, ok := h.seriesCache.Load(series.labels.String())
		if ok {
			buffer.batch.sampleInfos[i].seriesID = id.(SeriesID)
		} else {
			numMissingSeries += 1
		}
	}

	if numMissingSeries == 0 {
		return true
	}

	h.toSeriesInserter <- seriesInsertRequest{buffer, numMissingSeries}
	return false
}

func makeOrderedMap() orderedMap {
	return orderedMap{
		elements:   make(map[string]*list.Element),
		order:      list.List{},
		oldBuffers: make(chan *pendingBuffer, 1000),
	}
}

func (m *orderedMap) IsEmpty() bool {
	return len(m.elements) == 0
}

func (m *orderedMap) addReq(req insertDataRequest) (bool, *list.Element) {
	pending, ok := m.elements[req.metric]

	var needsFlush bool
	var buffer *pendingBuffer
	if ok {
		buffer = pending.Value.(*pendingBuffer)
	} else {
		buffer = m.newPendingBuffer(req.metric)
		pending = m.order.PushBack(buffer)
		m.elements[req.metric] = pending
	}

	needsFlush = buffer.addReq(req)
	return needsFlush, pending
}

func (p *pendingBuffer) addReq(req insertDataRequest) bool {
	p.needsResponse = append(p.needsResponse, insertDataTask{finished: req.finished, errChan: req.errChan})
	p.batch.sampleInfos = append(p.batch.sampleInfos, req.data...)
	return len(p.batch.sampleInfos) > flushSize
}

func (m *orderedMap) newPendingBuffer(metric string) *pendingBuffer {
	var buffer *pendingBuffer
	select {
	case buffer = <-m.oldBuffers:
		break
	default:
		buffer = &pendingBuffer{
			batch: SampleInfoIterator{sampleInfos: make([]labeledSamplesInfo, 0), sampleIndex: -1, sampleInfoIndex: 0},
		}
		break
	}

	buffer.start = time.Now()
	buffer.metric = metric
	return buffer
}

func (m *orderedMap) Front() (*list.Element, *pendingBuffer) {
	elem := m.order.Front()
	if elem == nil {
		return nil, nil
	}
	return elem, elem.Value.(*pendingBuffer)
}

func (m *orderedMap) Remove(elem *list.Element) *pendingBuffer {
	pending := elem.Value.(*pendingBuffer)
	m.order.Remove(elem)
	delete(m.elements, pending.metric)
	return pending
}

func (m *orderedMap) giveOldBuffer(buffer *pendingBuffer) {
	select {
	case m.oldBuffers <- buffer:
	default:
	}
}

// NewPgxReaderWithMetricCache returns a new DBReader that reads from PostgreSQL using PGX
// and caches metric table names using the supplied cacher.
func NewPgxReaderWithMetricCache(c *pgxpool.Pool, cache MetricCache) *DBReader {
	pi := &pgxQuerier{
		conn: &pgxConnImpl{
			conn: c,
		},
		metricTableNames: cache,
	}

	return &DBReader{
		db: pi,
	}
}

// NewPgxReader returns a new DBReader that reads that from PostgreSQL using PGX.
func NewPgxReader(c *pgxpool.Pool) *DBReader {
	metrics, _ := bigcache.NewBigCache(DefaultCacheConfig())
	cache := &MetricNameCache{metrics}
	return NewPgxReaderWithMetricCache(c, cache)
}

type metricTimeRangeFilter struct {
	metric    string
	startTime string
	endTime   string
}

type pgxQuerier struct {
	conn             pgxConn
	metricTableNames MetricCache
}

// HealthCheck implements the healtchecker interface
func (q *pgxQuerier) HealthCheck() error {
	rows, err := q.conn.Query(context.Background(), "SELECT")

	if err != nil {
		return err
	}

	rows.Close()
	return nil
}

func (q *pgxQuerier) Query(query *prompb.Query) ([]*prompb.TimeSeries, error) {
	if query == nil {
		return []*prompb.TimeSeries{}, nil
	}

	metric, cases, values, err := buildSubQueries(query)
	if err != nil {
		return nil, err
	}
	filter := metricTimeRangeFilter{
		metric:    metric,
		startTime: toRFC3339Nano(query.StartTimestampMs),
		endTime:   toRFC3339Nano(query.EndTimestampMs),
	}

	if metric != "" {
		return q.querySingleMetric(metric, filter, cases, values)
	}

	sqlQuery := buildMetricNameSeriesIDQuery(cases)
	rows, err := q.conn.Query(context.Background(), sqlQuery, values...)

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	metrics, series, err := getSeriesPerMetric(rows)

	if err != nil {
		return nil, err
	}

	results := make([]*prompb.TimeSeries, 0, len(metrics))

	for i, metric := range metrics {
		tableName, err := q.getMetricTableName(metric)
		if err != nil {
			// If the metric table is missing, there are no results for this query.
			if err == errMissingTableName {
				continue
			}

			return nil, err
		}
		filter.metric = tableName
		sqlQuery = buildTimeseriesBySeriesIDQuery(filter, series[i])
		rows, err = q.conn.Query(context.Background(), sqlQuery)

		if err != nil {
			return nil, err
		}

		ts, err := buildTimeSeries(rows)
		rows.Close()

		if err != nil {
			return nil, err
		}

		results = append(results, ts...)
	}

	return results, nil
}

func (q *pgxQuerier) querySingleMetric(metric string, filter metricTimeRangeFilter, cases []string, values []interface{}) ([]*prompb.TimeSeries, error) {
	tableName, err := q.getMetricTableName(metric)
	if err != nil {
		// If the metric table is missing, there are no results for this query.
		if err == errMissingTableName {
			return make([]*prompb.TimeSeries, 0), nil
		}

		return nil, err
	}
	filter.metric = tableName

	sqlQuery := buildTimeseriesByLabelClausesQuery(filter, cases)
	rows, err := q.conn.Query(context.Background(), sqlQuery, values...)

	if err != nil {
		// If we are getting undefined table error, it means the query
		// is looking for a metric which doesn't exist in the system.
		if e, ok := err.(*pgconn.PgError); !ok || e.Code != pgerrcode.UndefinedTable {
			return nil, err
		}
	}

	defer rows.Close()
	return buildTimeSeries(rows)
}

func (q *pgxQuerier) getMetricTableName(metric string) (string, error) {
	var err error
	var tableName string

	tableName, err = q.metricTableNames.Get(metric)

	if err == nil {
		return tableName, nil
	}

	if err != ErrEntryNotFound {
		return "", err
	}

	tableName, err = q.queryMetricTableName(metric)

	if err != nil {
		return "", err
	}

	err = q.metricTableNames.Set(metric, tableName)

	return tableName, err
}

func (q *pgxQuerier) queryMetricTableName(metric string) (string, error) {
	res, err := q.conn.Query(
		context.Background(),
		getMetricsTableSQL,
		metric,
	)

	if err != nil {
		return "", err
	}

	var tableName string
	defer res.Close()
	if !res.Next() {
		return "", errMissingTableName
	}

	if err := res.Scan(&tableName); err != nil {
		return "", err
	}

	return tableName, nil
}
