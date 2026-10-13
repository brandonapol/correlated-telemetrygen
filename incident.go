package main

import (
	"fmt"
	"time"
)

// An incident is a window in which the shop misbehaves, so an anomaly detector has something to find:
//   - errors:  the payment gateway times out on 90% of charges instead of --error-ratio
//   - latency: every payment gateway call takes 2–3 s longer, so payments' server spans do too
//   - drop:    90% of checkouts never happen, as if shoppers couldn't reach the site
//
// It starts --incident-after the run starts, lasts --incident-duration, and repeats every
// --incident-every if that is set.
type incidentKind string

const (
	incidentErrors  incidentKind = "errors"
	incidentLatency incidentKind = "latency"
	incidentDrop    incidentKind = "drop"
)

const (
	incidentErrorRatio = 0.9 // the gateway's failure ratio during an errors incident
	incidentDropRatio  = 0.9 // the fraction of checkouts a drop incident removes
)

type incident struct {
	kind     incidentKind
	after    time.Duration // from the start of the run; 0 means no incident
	duration time.Duration
	every    time.Duration // 0 means once
}

func (i incident) validate() error {
	if i.after == 0 {
		return nil
	}
	switch i.kind {
	case incidentErrors, incidentLatency, incidentDrop:
	default:
		return fmt.Errorf("--incident-kind must be errors, latency or drop, not %q", i.kind)
	}
	if i.after < 0 || i.duration <= 0 {
		return fmt.Errorf("--incident-after and --incident-duration must be positive")
	}
	if i.every != 0 && i.every <= i.duration {
		return fmt.Errorf("--incident-every must be longer than --incident-duration")
	}
	return nil
}

// at is the kind of incident under way elapsed after the start of the run, or "" when there is none.
func (i incident) at(elapsed time.Duration) incidentKind {
	if i.after <= 0 || elapsed < i.after {
		return ""
	}
	since := elapsed - i.after
	if i.every > 0 {
		since %= i.every
	}
	if since >= i.duration {
		return ""
	}
	return i.kind
}

// drops reports whether a drop incident under way removes this checkout; r is uniform in [0, 1).
func (i incident) drops(elapsed time.Duration, r float64) bool {
	return i.at(elapsed) == incidentDrop && r < incidentDropRatio
}
