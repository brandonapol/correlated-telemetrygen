package main

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestIncidentWindow(t *testing.T) {
	once := incident{kind: incidentErrors, after: 10 * time.Minute, duration: 5 * time.Minute}
	repeating := incident{kind: incidentLatency, after: 10 * time.Minute, duration: 5 * time.Minute, every: 30 * time.Minute}
	for _, tc := range []struct {
		name    string
		inc     incident
		elapsed time.Duration
		want    incidentKind
	}{
		{"none configured", incident{kind: incidentErrors, duration: time.Minute}, time.Hour, ""},
		{"before it starts", once, 9 * time.Minute, ""},
		{"as it starts", once, 10 * time.Minute, incidentErrors},
		{"during", once, 14 * time.Minute, incidentErrors},
		{"as it ends", once, 15 * time.Minute, ""},
		{"long after a single incident", once, 45 * time.Minute, ""},
		{"between repeats", repeating, 20 * time.Minute, ""},
		{"the second repeat", repeating, 42 * time.Minute, incidentLatency},
	} {
		if got := tc.inc.at(tc.elapsed); got != tc.want {
			t.Errorf("%s: at(%s) = %q, want %q", tc.name, tc.elapsed, got, tc.want)
		}
	}
}

func TestIncidentValidation(t *testing.T) {
	for _, tc := range []struct {
		inc incident
		ok  bool
	}{
		{incident{}, true},
		{incident{kind: incidentDrop, after: time.Minute, duration: time.Minute}, true},
		{incident{kind: "outage", after: time.Minute, duration: time.Minute}, false},
		{incident{kind: incidentErrors, after: time.Minute}, false},
		{incident{kind: incidentErrors, after: time.Minute, duration: time.Minute, every: time.Minute}, false},
	} {
		if err := tc.inc.validate(); (err == nil) != tc.ok {
			t.Errorf("validate(%+v) = %v, want ok=%v", tc.inc, err, tc.ok)
		}
	}
}

func TestDropIncidentRemovesMostCheckoutsOnlyWhileItLasts(t *testing.T) {
	inc := incident{kind: incidentDrop, after: time.Minute, duration: time.Minute}
	rng := rand.New(rand.NewPCG(1, 2))
	dropped := 0
	for range 1000 {
		if inc.drops(90*time.Second, rng.Float64()) {
			dropped++
		}
		if inc.drops(30*time.Second, rng.Float64()) || inc.drops(3*time.Minute, rng.Float64()) {
			t.Fatal("a drop incident removed a checkout outside its window")
		}
	}
	if dropped < 850 || dropped > 950 {
		t.Fatalf("dropped %d of 1000 checkouts, want about 900", dropped)
	}
}

// simulateDuring runs n interactions with an incident under way and returns payments' server spans.
func simulateDuring(t *testing.T, n int, kind incidentKind) []sdktrace.ReadOnlySpan {
	t.Helper()
	spans := tracetest.NewSpanRecorder()
	logs := &logRecorder{}
	rng := rand.New(rand.NewPCG(1, 2))
	sim := newSimulator(
		recordingFleet(t, browserResources(testDeployment), spans, logs),
		recordingFleet(t, replicaResources(checkoutIdentity, testDeployment, rng), spans, logs),
		recordingFleet(t, replicaResources(paymentsIdentity, testDeployment, rng), spans, logs),
		withoutDBErrors, rng,
	)
	sim.incident = kind
	start := time.Unix(1_700_000_000, 0)
	for i := range n {
		sim.run(context.Background(), start.Add(time.Duration(i)*time.Second))
	}
	var payments []sdktrace.ReadOnlySpan
	for _, s := range spans.Ended() {
		if s.SpanKind() == trace.SpanKindServer && serviceOf(s.Resource()) == "payments" {
			payments = append(payments, s)
		}
	}
	return payments
}

func TestErrorsIncidentFailsMostCharges(t *testing.T) {
	payments := simulateDuring(t, 400, incidentErrors)
	failed := 0
	for _, s := range payments {
		if s.Status().Code == codes.Error {
			failed++
		}
	}
	if ratio := float64(failed) / float64(len(payments)); ratio < 0.85 || ratio > 0.95 {
		t.Fatalf("payments error ratio %.2f during an errors incident, want about 0.9", ratio)
	}
}

func TestLatencyIncidentSlowsEveryCharge(t *testing.T) {
	for _, s := range simulateDuring(t, 200, incidentLatency) {
		if d := s.EndTime().Sub(s.StartTime()); d < 2*time.Second {
			t.Fatalf("a payments server span took %s during a latency incident, want over 2s", d)
		}
	}
	for _, s := range simulateDuring(t, 200, "") {
		if d := s.EndTime().Sub(s.StartTime()); d > 1500*time.Millisecond {
			t.Fatalf("a payments server span took %s with no incident, want under 1.5s", d)
		}
	}
}
