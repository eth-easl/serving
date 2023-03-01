/*
Copyright 2018 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scaling

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	pkgmetrics "knative.dev/pkg/metrics"
	"knative.dev/serving/pkg/apis/autoscaling"
	"knative.dev/serving/pkg/autoscaler/aggregation/max"
	"knative.dev/serving/pkg/autoscaler/metrics"
	"knative.dev/serving/pkg/resources"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

type podCounter interface {
	ReadyCount() (int, error)
}

// autoscaler stores current state of an instance of an autoscaler.
type autoscaler struct {
	namespace    string
	revision     string
	metricClient metrics.MetricClient
	podCounter   podCounter
	reporterCtx  context.Context

	// State in panic mode.
	panicTime    time.Time
	maxPanicPods int32

	// delayWindow is used to defer scale-down decisions until a time
	// window has passed at the reduced concurrency.
	delayWindow *max.TimeWindow

	// specMux guards the current DeciderSpec.
	specMux     sync.RWMutex
	deciderSpec *DeciderSpec

	// hybrid policy
	invocationsPerMinute       []float64
	processedRequestsPerMinute []float64
	capacityEstimateWindow     []float64
	startTime                  time.Time
	currentMinute              int
	windowResized              bool
	previousReadyPodsCount     float64
	averageCapacity            float64
	previousPrediction         float64
	pastAverageCapacityValues  []float64
	stability                  float64
}

// New creates a new instance of default autoscaler implementation.
func New(
	reporterCtx context.Context,
	namespace, revision string,
	metricClient metrics.MetricClient,
	podCounter resources.EndpointsCounter,
	deciderSpec *DeciderSpec) UniScaler {

	var delayer *max.TimeWindow
	if deciderSpec.ScaleDownDelay > 0 {
		delayer = max.NewTimeWindow(deciderSpec.ScaleDownDelay, tickInterval)
	}

	return newAutoscaler(reporterCtx, namespace, revision, metricClient,
		podCounter, deciderSpec, delayer)
}

func newAutoscaler(
	reporterCtx context.Context,
	namespace, revision string,
	metricClient metrics.MetricClient,
	podCounter podCounter,
	deciderSpec *DeciderSpec,
	delayWindow *max.TimeWindow) *autoscaler {

	// We always start in the panic mode, if the deployment is scaled up over 1 pod.
	// If the scale is 0 or 1, normal Autoscaler behavior is fine.
	// When Autoscaler restarts we lose metric history, which causes us to
	// momentarily scale down, and that is not a desired behaviour.
	// Thus, we're keeping at least the current scale until we
	// accumulate enough data to make conscious decisions.
	curC, err := podCounter.ReadyCount()
	if err != nil {
		// This always happens on new revision creation, since decider
		// is reconciled before SKS has even chance of creating the service/endpoints.
		curC = 0
	}
	var pt time.Time
	if curC > 1 {
		pt = time.Now()
		// A new instance of autoscaler is created in panic mode.
		pkgmetrics.Record(reporterCtx, panicM.M(1))
	} else {
		pkgmetrics.Record(reporterCtx, panicM.M(0))
	}

	return &autoscaler{
		namespace:    namespace,
		revision:     revision,
		metricClient: metricClient,
		reporterCtx:  reporterCtx,

		deciderSpec: deciderSpec,
		podCounter:  podCounter,

		delayWindow: delayWindow,

		panicTime:    pt,
		maxPanicPods: int32(curC),
	}
}

// Update reconfigures the UniScaler according to the DeciderSpec.
func (a *autoscaler) Update(deciderSpec *DeciderSpec) {
	a.specMux.Lock()
	defer a.specMux.Unlock()

	a.deciderSpec = deciderSpec
}

// Scale calculates the desired scale based on current statistics given the current time.
// desiredPodCount is the calculated pod count the autoscaler would like to set.
// validScale signifies whether the desiredPodCount should be applied or not.
// Scale is not thread safe in regards to panic state, but it's thread safe in
// regards to acquiring the decider spec.
func (a *autoscaler) Scale(logger *zap.SugaredLogger, now time.Time) ScaleResult {
	desugared := logger.Desugar()
	debugEnabled := desugared.Core().Enabled(zapcore.DebugLevel)

	spec := a.currentSpec()
	originalReadyPodsCount, err := a.podCounter.ReadyCount()
	// If the error is NotFound, then presume 0.
	if err != nil && !apierrors.IsNotFound(err) {
		logger.Errorw("Failed to get ready pod count via K8S Lister", zap.Error(err))
		return invalidSR
	}
	// Use 1 if there are zero current pods.
	readyPodsCount := math.Max(1, float64(originalReadyPodsCount))

	metricKey := types.NamespacedName{Namespace: a.namespace, Name: a.revision}

	metricName := spec.ScalingMetric
	var observedStableValue, observedPanicValue float64
	var dspc, dppc float64
	switch spec.ScalingMetric {
	case autoscaling.RPS:
		observedStableValue, observedPanicValue, err = a.metricClient.StableAndPanicRPS(metricKey, now)
	case autoscaling.Hybrid:
		dspc = a.hybridScaling(readyPodsCount, metricKey, now, logger)
		if dspc == -1 {
			return invalidSR
		}
	default:
		metricName = autoscaling.Concurrency // concurrency is used by default
		observedStableValue, observedPanicValue, err = a.metricClient.StableAndPanicConcurrency(metricKey, now)
	}

	if err != nil {
		if errors.Is(err, metrics.ErrNoData) {
			logger.Debug("No data to scale on yet")
		} else {
			logger.Errorw("Failed to obtain metrics", zap.Error(err))
		}
		return invalidSR
	}

	// Make sure we don't get stuck with the same number of pods, if the scale up rate
	// is too conservative and MaxScaleUp*RPC==RPC, so this permits us to grow at least by a single
	// pod if we need to scale up.
	// E.g. MSUR=1.1, OCC=3, RPC=2, TV=1 => OCC/TV=3, MSU=2.2 => DSPC=2, while we definitely, need
	// 3 pods. See the unit test for this scenario in action.
	maxScaleUp := math.Ceil(spec.MaxScaleUpRate * readyPodsCount)
	// Same logic, opposite math applies here.
	maxScaleDown := 0.
	if spec.Reachable {
		maxScaleDown = math.Floor(readyPodsCount / spec.MaxScaleDownRate)
	}
	if spec.ScalingMetric == autoscaling.Concurrency || spec.ScalingMetric == autoscaling.RPS {
		dspc = math.Ceil(observedStableValue / spec.TargetValue)
		dppc = math.Ceil(observedPanicValue / spec.TargetValue)
		if debugEnabled {
			desugared.Debug(
				fmt.Sprintf("For metric %s observed values: stable = %0.3f; panic = %0.3f; target = %0.3f "+
					"Desired StablePodCount = %0.0f, PanicPodCount = %0.0f, ReadyEndpointCount = %d, MaxScaleUp = %0.0f, MaxScaleDown = %0.0f",
					metricName, observedStableValue, observedPanicValue, spec.TargetValue,
					dspc, dppc, originalReadyPodsCount, maxScaleUp, maxScaleDown))
		}
	}

	// We want to keep desired pod count in the  [maxScaleDown, maxScaleUp] range.
	desiredStablePodCount := int32(math.Min(math.Max(dspc, maxScaleDown), maxScaleUp))
	desiredPanicPodCount := int32(math.Min(math.Max(dppc, maxScaleDown), maxScaleUp))

	if spec.ScalingMetric == autoscaling.Concurrency || spec.ScalingMetric == autoscaling.RPS {
		isOverPanicThreshold := dppc/readyPodsCount >= spec.PanicThreshold

		if a.panicTime.IsZero() && isOverPanicThreshold {
			// Begin panicking when we cross the threshold in the panic window.
			logger.Info("PANICKING.")
			a.panicTime = now
			pkgmetrics.Record(a.reporterCtx, panicM.M(1))
		} else if isOverPanicThreshold {
			// If we're still over panic threshold right now — extend the panic window.
			a.panicTime = now
		} else if !a.panicTime.IsZero() && !isOverPanicThreshold && a.panicTime.Add(spec.StableWindow).Before(now) {
			// Stop panicking after the surge has made its way into the stable metric.
			logger.Info("Un-panicking.")
			a.panicTime = time.Time{}
			a.maxPanicPods = 0
			pkgmetrics.Record(a.reporterCtx, panicM.M(0))
		}
	}
	desiredPodCount := desiredStablePodCount
	if spec.ScalingMetric == autoscaling.Concurrency || spec.ScalingMetric == autoscaling.RPS {

		if !a.panicTime.IsZero() {
			// In some edgecases stable window metric might be larger
			// than panic one. And we should provision for stable as for panic,
			// so pick the larger of the two.
			if desiredPodCount < desiredPanicPodCount {
				desiredPodCount = desiredPanicPodCount
			}
			logger.Debug("Operating in panic mode.")
			// We do not scale down while in panic mode. Only increases will be applied.
			if desiredPodCount > a.maxPanicPods {
				logger.Infof("Increasing pods count from %d to %d.", originalReadyPodsCount, desiredPodCount)
				a.maxPanicPods = desiredPodCount
			} else if desiredPodCount < a.maxPanicPods {
				logger.Infof("Skipping pod count decrease from %d to %d.", a.maxPanicPods, desiredPodCount)
			}
			desiredPodCount = a.maxPanicPods
		} else {
			logger.Debug("Operating in stable mode.")
		}

		// Delay scale down decisions, if a ScaleDownDelay was specified.
		// We only do this if there's a non-nil delayWindow because although a
		// one-element delay window is _almost_ the same as no delay at all, it is
		// not the same in the case where two Scale()s happen in the same time
		// interval (because the largest will be picked rather than the most recent
		// in that case).
		if a.delayWindow != nil {
			a.delayWindow.Record(now, desiredPodCount)
			delayedPodCount := a.delayWindow.Current()
			if delayedPodCount != desiredPodCount {
				if debugEnabled {
					desugared.Debug(
						fmt.Sprintf("Delaying scale to %d, staying at %d",
							desiredPodCount, delayedPodCount))
				}
				desiredPodCount = delayedPodCount
			}
		}
	}
	// Compute excess burst capacity
	//
	// the excess burst capacity is based on panic value, since we don't want to
	// be making knee-jerk decisions about Activator in the request path.
	// Negative EBC means that the deployment does not have enough capacity to serve
	// the desired burst off hand.
	// EBC = TotCapacity - Cur#ReqInFlight - TargetBurstCapacity
	excessBCF := -1.
	switch {
	case spec.TargetBurstCapacity == 0:
		excessBCF = 0
	case spec.TargetBurstCapacity > 0:
		totCap := float64(originalReadyPodsCount) * spec.TotalValue
		excessBCF = math.Floor(totCap - spec.TargetBurstCapacity - observedPanicValue)
		// TODO: observed panic value is always 0 for hybrid, is that an issue?
	}

	if debugEnabled {
		desugared.Debug(fmt.Sprintf("PodCount=%d Total1PodCapacity=%0.3f ObsStableValue=%0.3f ObsPanicValue=%0.3f TargetBC=%0.3f ExcessBC=%0.3f",
			originalReadyPodsCount, spec.TotalValue, observedStableValue,
			observedPanicValue, spec.TargetBurstCapacity, excessBCF))
	}

	switch spec.ScalingMetric {
	case autoscaling.RPS:
		pkgmetrics.RecordBatch(a.reporterCtx,
			excessBurstCapacityM.M(excessBCF),
			desiredPodCountM.M(int64(desiredPodCount)),
			stableRPSM.M(observedStableValue),
			panicRPSM.M(observedStableValue),
			targetRPSM.M(spec.TargetValue),
		)
	case autoscaling.Hybrid:
		pkgmetrics.RecordBatch(a.reporterCtx,
			excessBurstCapacityM.M(excessBCF),
			desiredPodCountM.M(int64(desiredPodCount)),
			stableRequestConcurrencyM.M(-1),
			panicRequestConcurrencyM.M(-1),
			targetRequestConcurrencyM.M(-1),
			// TODO: think about what to record
		)
	default:
		pkgmetrics.RecordBatch(a.reporterCtx,
			excessBurstCapacityM.M(excessBCF),
			desiredPodCountM.M(int64(desiredPodCount)),
			stableRequestConcurrencyM.M(observedStableValue),
			panicRequestConcurrencyM.M(observedPanicValue),
			targetRequestConcurrencyM.M(spec.TargetValue),
		)
	}

	return ScaleResult{
		DesiredPodCount:     desiredPodCount,
		ExcessBurstCapacity: int32(excessBCF),
		ScaleValid:          true,
	}
}

func (a *autoscaler) currentSpec() *DeciderSpec {
	a.specMux.RLock()
	defer a.specMux.RUnlock()
	return a.deciderSpec
}

func (a *autoscaler) hybridScaling(readyPodsCount float64, metricKey types.NamespacedName,
	now time.Time, logger *zap.SugaredLogger) float64 {
	if a.startTime.IsZero() {
		a.startTime = now
		a.currentMinute = 0
		logger.Infof("current minute is 0")

	}
	prevMinute := a.currentMinute
	a.currentMinute = int(now.Sub(a.startTime).Minutes())
	logger.Infof("current minute: %d", a.currentMinute)

	if prevMinute < a.currentMinute {
		observedRps, _, err := a.metricClient.StableAndPanicRPS(metricKey, now)
		if err != nil {
			if err == metrics.ErrNoData {
				logger.Debug("0 invocations for previous minute")
				// observed rps will be 0 then, so this is ok
			} else {
				logger.Errorw("Failed to obtain metrics", zap.Error(err))
				return -1
				// -1 is interpreted as invalid scale
			}
		}
		a.invocationsPerMinute = append(a.invocationsPerMinute, observedRps*60)
		// multiply by 60 to get the number of invocations for that minute
	}

	processedRequests, err := a.metricClient.ProcessedRequestsEstimate(metricKey, now)
	logger.Infof("processed requests: %f", processedRequests)
	if err != nil {
		if err == metrics.ErrNoData {
			logger.Debug("No requests processed yet")
			// log but just continue, because it's normal that before we scale there are no processed requests
		} else {
			logger.Errorw("Failed to obtain metrics", zap.Error(err))
			return -1
			// -1 is interpreted as invalid scale
		}
	}

	if a.currentMinute+1 > len(a.processedRequestsPerMinute) {
		a.processedRequestsPerMinute = append(a.processedRequestsPerMinute, processedRequests)
	} else {
		a.processedRequestsPerMinute[a.currentMinute] += processedRequests
		// add to current minute
	}
	observedConcurrency, _, err := a.metricClient.StableAndPanicConcurrency(metricKey, now)
	if err != nil {
		if err == metrics.ErrNoData {
			logger.Debug("No requests in the system currently")
			// log but just continue, we might want to scale up anyways based on predictions
		} else {
			logger.Errorw("Failed to obtain metrics", zap.Error(err))
			return -1
			// -1 is interpreted as invalid scale
		}
	}
	var desiredScale float64
	if a.currentMinute < 60 {
		desiredScale = math.Ceil(observedConcurrency)
		// purely concurrency based scaling for first 60 minutes

		// capacity estimate should be computed here, stop computing it after warmup period of 60 min
		averagePodCount := (readyPodsCount + a.previousReadyPodsCount) / 2
		a.previousReadyPodsCount = readyPodsCount
		// previous ready pod count might be 0, but it's reasonable to assume we didn't have the current
		// ready pod count for the entire 2 second period
		capacity := (processedRequests / 2) / averagePodCount
		// processed requests is the number of requests processed in the last 2 seconds, so divide by 2 to get the
		// average number processed per second
		a.capacityEstimateWindow = append(a.capacityEstimateWindow, capacity)
		if prevMinute < a.currentMinute {
			if a.processedRequestsPerMinute[prevMinute] > 60 &&
				a.processedRequestsPerMinute[prevMinute]/a.invocationsPerMinute[prevMinute] < 1.2 &&
				a.processedRequestsPerMinute[prevMinute]/a.invocationsPerMinute[prevMinute] > 0.8 {
				// number of processed requests is high enough to get decent estimate
				// number of processed requests does not deviate from the invocations during that minute by too much
				var sum float64
				for i := 0; i < len(a.capacityEstimateWindow); i++ {
					sum += a.capacityEstimateWindow[i]
				}
				averagePrevMinute := sum / float64(len(a.capacityEstimateWindow))
				if a.averageCapacity == 0 {
					a.averageCapacity = averagePrevMinute
				} else {
					a.averageCapacity = a.averageCapacity*0.8 + averagePrevMinute*0.2
				}
				a.pastAverageCapacityValues = append(a.pastAverageCapacityValues, averagePrevMinute)
				logger.Infof("Average capacity previous minute %f average capacity %f",
					averagePrevMinute, a.averageCapacity)
			} else {
				a.capacityEstimateWindow = nil
				// reset capacity estimate window
			}
			logger.Infof("Processed requests: %f invocations this minute: %f",
				a.processedRequestsPerMinute[prevMinute], a.invocationsPerMinute[prevMinute])
		}

	} else {
		if !a.windowResized {
			err := a.resizeWindow(metricKey, logger)
			if err != nil {
				logger.Errorw("Failed to resize window", zap.Error(err))
				return -1
				// -1 is interpreted as invalid scale
			}
			variance, mean := ComputeVariance(a.pastAverageCapacityValues)
			if variance > 0 {
				a.stability = mean / math.Sqrt(variance)
			}
		}
		var prediction float64
		if prevMinute < a.currentMinute {
			prediction = fourierExtrapolation(a.invocationsPerMinute, 30)
			a.previousPrediction = prediction
		} else {
			prediction = a.previousPrediction
		}
		desiredPredictedScale := (1 / a.averageCapacity) * prediction / 60
		if a.stability > 1 {
			desiredScale = math.Ceil(math.Max(observedConcurrency, desiredPredictedScale))
			logger.Infof("Stability: %f observed concurrency: %f predicted scale: %f",
				a.stability, observedConcurrency, desiredPredictedScale)
		} else if prediction >= 1 {
			desiredScale = math.Ceil(math.Max(observedConcurrency, 1))
			logger.Infof("Stability: %f observed concurrency: %f prediction: 1",
				a.stability, observedConcurrency)
		} else {
			desiredScale = math.Ceil(observedConcurrency)
			logger.Infof("Stability: %f observed concurrency: %f",
				a.stability, observedConcurrency)
		}
	}
	logger.Infof("Desired scale is %f", desiredScale)
	return math.Ceil(desiredScale)
}

func (a *autoscaler) resizeWindow(metricKey types.NamespacedName, logger *zap.SugaredLogger) error {
	variance, mean := ComputeVariance(a.invocationsPerMinute)
	var windowInSeconds time.Duration
	if variance == 0 || mean == 0 {
		windowInSeconds = 60 * time.Second
	} else {
		std := math.Sqrt(variance)
		windowInSeconds = time.Duration((math.Round(20 * mean / std)) * float64(time.Second))
	}
	if windowInSeconds > 600 {
		// maximum window size should be 10 minutes (600 seconds)
		windowInSeconds = 600 * time.Second
	} else if windowInSeconds < 20 {
		// less than 20 seconds is too noisy
		windowInSeconds = 20 * time.Second
	}
	logger.Infof("Setting window size to %f seconds", windowInSeconds.Seconds())
	err := a.metricClient.ResizeConcurrencyWindow(metricKey, windowInSeconds)
	return err
}
