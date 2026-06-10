package appdefaults

import "time"

const (
	// The session healthcheck exists to reap sessions whose client died.
	// It must tolerate a merely SLOW client: on saturated CI runners the
	// client process (earth) can be starved of CPU for tens of seconds
	// (go build/link, parallel tests), and killing its session surfaces as
	// "BuildKit canceled or lost the solve session" with cancellation
	// fan-out through every dependent op. One missed 10s check was far too
	// hair-triggered; ~90s of consecutive failures now means actually dead.
	HealthAllowedFailures = 3
	HealthFrequency       = 1 * time.Second
	HealthTimeout         = 30 * time.Second
)
