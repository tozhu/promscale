// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package model

import (
	"context"
	"math"
	"time"

	"github.com/jackc/pgtype"
	"go.opentelemetry.io/collector/model/pdata"
)

const (
	MetricNameLabelName = "__name__"
	SchemaNameLabelName = "__schema__"
	ColumnNameLabelName = "__column__"
)

var (
	MinTime = time.Unix(math.MinInt64/1000+62135596801, 0).UTC()
	MaxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()
)

type Metadata struct {
	MetricFamily string `json:"metric,omitempty"`
	Unit         string `json:"unit"`
	Type         string `json:"type"`
	Help         string `json:"help"`
}

// Dispatcher is responsible for inserting label, series and data into the storage.
type Dispatcher interface {
	InsertSpanLinks(ctx context.Context, links pdata.SpanLinkSlice, traceID [16]byte, spanID [8]byte, spanStartTime time.Time) error
	InsertSpanEvents(ctx context.Context, events pdata.SpanEventSlice, traceID [16]byte, spanID [8]byte) error
	InsertSpan(ctx context.Context, span pdata.Span, nameID, instLibID, rSchemaURLID pgtype.Int8, resourceTags pdata.AttributeMap) error
	InsertSchemaURL(ctx context.Context, sURL string) (id pgtype.Int8, err error)
	InsertSpanName(ctx context.Context, name string) (id pgtype.Int8, err error)
	InsertInstrumentationLibrary(ctx context.Context, name, version, sURL string) (id pgtype.Int8, err error)
	InsertTs(rows Data) (uint64, error)
	InsertMetadata([]Metadata) (uint64, error)
	CompleteMetricCreation() error
	Close()
}

func TimestamptzToMs(t pgtype.Timestamptz) int64 {
	switch t.InfinityModifier {
	case pgtype.NegativeInfinity:
		return math.MinInt64
	case pgtype.Infinity:
		return math.MaxInt64
	default:
		return t.Time.UnixNano() / 1e6
	}
}
