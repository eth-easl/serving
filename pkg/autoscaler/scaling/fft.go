package scaling

import (
	"github.com/mjibson/go-dsp/fft"
	"github.com/openacid/slimarray/polyfit"
	"golang.org/x/exp/slices"
	"math"
	"math/cmplx"
)

func fourierExtrapolation(xFloats []float64, harmonics int) float64 {
	n := len(xFloats)
	harmonics = int(math.Min(float64(n-1), float64(harmonics)))
	// harmonics should always be smaller than n, harmonics equal to n-1 means taking all frequencies
	var t []float64
	for i := 0; i < n; i++ {
		t = append(t, float64(i))
	}
	f := polyfit.NewFit(t, xFloats, 1)
	p := f.Solve()
	var xNotrend []float64
	for i := 0; i < n; i++ {
		xNotrend = append(xNotrend, xFloats[i]-p[1]*t[i])
	}
	xFreqdom := fft.FFTReal(xNotrend)
	var indices []int
	for i := 0; i < harmonics+1; i++ {
		if !slices.Contains(indices, i) {
			indices = append(indices, i)
		}
	}
	for i := n - 1; i >= n-harmonics; i-- {
		if !slices.Contains(indices, i) {
			indices = append(indices, i)
		}
	}
	var restoredSig float64
	for i := 0; i < len(indices); i++ {
		amplitude, phase := cmplx.Polar(xFreqdom[indices[i]])
		amplitude = amplitude / float64(n)
		restoredSig += amplitude * math.Cos(2*math.Pi*float64(indices[i]*n)+phase)
	}
	return restoredSig + p[1]*float64(n)
}

func ComputeVariance(x []float64) (float64, float64) {
	signalLength := len(x)
	if signalLength == 0 {
		// return negative variance to show that the result is unusable
		return -1, -1
	}
	sum := 0.0
	sumSquared := 0.0
	for i := 0; i < signalLength; i++ {
		sum += x[i]
		sumSquared += x[i] * x[i]
	}
	N := float64(signalLength)
	mean := sum / N
	return (sumSquared - sum*sum/N) / (N - 1.0), mean
}
