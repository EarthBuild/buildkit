package session

import (
	"context"
	"time"

	"github.com/moby/buildkit/util/bklog"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func configurableMonitorHealth(ctx context.Context, cc *grpc.ClientConn, cancelConn func(error), healthCfg ManagerHealthCfg) {
	var cancelCause error
	defer func() { cancelConn(cancelCause) }()
	defer cc.Close()

	ticker := time.NewTicker(healthCfg.frequency)
	defer ticker.Stop()
	healthClient := grpc_health_v1.NewHealthClient(cc)

	consecutiveFailures := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			timeoutStart := time.Now().UTC()

			ctx, cancel := context.WithTimeoutCause(ctx, healthCfg.timeout, nil)
			_, err := healthClient.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
			cancel()

			logFields := logrus.Fields{
				"timeout":        healthCfg.timeout,
				"actualDuration": time.Since(timeoutStart),
			}

			if err != nil {
				consecutiveFailures++

				logFields["allowedFailures"] = healthCfg.allowedFailures
				logFields["consecutiveFailures"] = consecutiveFailures
				bklog.G(ctx).WithFields(logFields).Warn("healthcheck failed")

				if consecutiveFailures >= healthCfg.allowedFailures {
					cancelCause = errors.Wrapf(err, "session healthcheck failed too many times after %d consecutive failures", consecutiveFailures)
					bklog.G(ctx).WithError(cancelCause).Error("healthcheck failed too many times")
					return
				}
			} else {
				bklog.G(ctx).WithFields(logFields).Debug("healthcheck completed")
				consecutiveFailures = 0
			}
		}
	}
}
