package main

import (
	"context"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// logRecorder keeps every emitted log record in memory.
type logRecorder struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func (r *logRecorder) Enabled(context.Context, sdklog.EnabledParameters) bool { return true }
func (r *logRecorder) Shutdown(context.Context) error                         { return nil }
func (r *logRecorder) ForceFlush(context.Context) error                       { return nil }
func (r *logRecorder) OnEmit(_ context.Context, rec *sdklog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, rec.Clone())
	return nil
}

func recordingService(t *testing.T, name string, spans *tracetest.SpanRecorder, logs *logRecorder) service {
	t.Helper()
	res := resource.NewSchemaless(attribute.String("service.name", name))
	tp := sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithSpanProcessor(spans))
	lp := sdklog.NewLoggerProvider(sdklog.WithResource(res), sdklog.WithProcessor(logs))
	return service{tracer: tp.Tracer(scope), logger: lp.Logger(scope)}
}

func simulate(t *testing.T, n int, errorRatio float64) ([]sdktrace.ReadOnlySpan, []sdklog.Record) {
	t.Helper()
	spans := tracetest.NewSpanRecorder()
	logs := &logRecorder{}
	sim := &simulator{
		checkout:   recordingService(t, "checkout", spans, logs),
		payments:   recordingService(t, "payments", spans, logs),
		errorRatio: errorRatio,
		rng:        rand.New(rand.NewPCG(1, 2)),
	}
	start := time.Unix(1_700_000_000, 0)
	for i := range n {
		sim.run(context.Background(), start.Add(time.Duration(i)*time.Second))
	}
	return spans.Ended(), logs.records
}

func serviceOf(res *resource.Resource) string {
	v, _ := res.Set().Value("service.name")
	return v.AsString()
}

func attr(rec sdklog.Record, key string) (attribute.Value, bool) {
	var found attribute.Value
	ok := false
	rec.WalkAttributes(func(kv attribute.KeyValue) bool {
		if string(kv.Key) == key {
			found, ok = kv.Value, true
			return false
		}
		return true
	})
	return found, ok
}

func TestEveryLogBelongsToASpanOfTheSameServiceAndTrace(t *testing.T) {
	spans, logs := simulate(t, 50, 0.25)
	if len(logs) == 0 {
		t.Fatal("no logs emitted")
	}
	byID := map[trace.SpanID]sdktrace.ReadOnlySpan{}
	for _, s := range spans {
		byID[s.SpanContext().SpanID()] = s
	}
	for _, rec := range logs {
		span, ok := byID[rec.SpanID()]
		if !ok {
			t.Fatalf("log %q has span ID %s, which no span has", rec.Body().AsString(), rec.SpanID())
		}
		if span.SpanContext().TraceID() != rec.TraceID() {
			t.Fatalf("log %q is in trace %s but its span is in %s", rec.Body().AsString(), rec.TraceID(), span.SpanContext().TraceID())
		}
		if serviceOf(span.Resource()) != serviceOf(rec.Resource()) {
			t.Fatalf("log %q from %s sits in a %s span", rec.Body().AsString(), serviceOf(rec.Resource()), serviceOf(span.Resource()))
		}
		if v, _ := attr(rec, "trace_id"); v.AsString() != rec.TraceID().String() {
			t.Fatalf("trace_id attribute %q does not match the record's trace ID %s", v.AsString(), rec.TraceID())
		}
		if v, _ := attr(rec, "span_id"); v.AsString() != rec.SpanID().String() {
			t.Fatalf("span_id attribute %q does not match the record's span ID %s", v.AsString(), rec.SpanID())
		}
		if ts := rec.Timestamp(); ts.Before(span.StartTime()) || ts.After(span.EndTime()) {
			t.Fatalf("log %q at %s falls outside its span [%s, %s]", rec.Body().AsString(), ts, span.StartTime(), span.EndTime())
		}
	}
}

func TestPaymentsServerSpanIsAChildOfCheckoutsClientCall(t *testing.T) {
	spans, _ := simulate(t, 10, 0.25)
	byID := map[trace.SpanID]sdktrace.ReadOnlySpan{}
	for _, s := range spans {
		byID[s.SpanContext().SpanID()] = s
	}
	found := 0
	for _, s := range spans {
		if serviceOf(s.Resource()) != "payments" || s.SpanKind() != trace.SpanKindServer {
			continue
		}
		found++
		parent, ok := byID[s.Parent().SpanID()]
		if !ok || serviceOf(parent.Resource()) != "checkout" || parent.SpanKind() != trace.SpanKindClient {
			t.Fatalf("payments server span's parent is not a checkout client span")
		}
	}
	if found != 10 {
		t.Fatalf("got %d payments server spans, want 10", found)
	}
}

func TestEachServiceEmitsOneWideEventPerRequest(t *testing.T) {
	const n = 20
	_, logs := simulate(t, n, 0.25)
	perService := map[string]int{}
	for _, rec := range logs {
		if v, ok := attr(rec, "main"); !ok || !v.AsBool() {
			continue
		}
		perService[serviceOf(rec.Resource())]++
		for _, key := range []string{"http.route", "http.response.status_code", "duration_ms", "user.id", "user.team.id", "db.query_count", "cache.hit", "outcome"} {
			if _, ok := attr(rec, key); !ok {
				t.Fatalf("%s wide event is missing %s", serviceOf(rec.Resource()), key)
			}
		}
	}
	if perService["checkout"] != n || perService["payments"] != n {
		t.Fatalf("wide events per service = %v, want %d each", perService, n)
	}
}

func TestPaymentErrorsStayOutOfCheckoutsServerSpan(t *testing.T) {
	spans, _ := simulate(t, 400, 0.25)
	var payments, paymentErrors int
	for _, s := range spans {
		if s.SpanKind() != trace.SpanKindServer {
			continue
		}
		switch serviceOf(s.Resource()) {
		case "checkout":
			if s.Status().Code == codes.Error {
				t.Fatal("a checkout server span has error status")
			}
		case "payments":
			payments++
			if s.Status().Code == codes.Error {
				paymentErrors++
			}
		}
	}
	if ratio := float64(paymentErrors) / float64(payments); ratio < 0.18 || ratio > 0.32 {
		t.Fatalf("payments error ratio %.2f, want about 0.25", ratio)
	}
}
