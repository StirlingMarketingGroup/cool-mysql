package mysql

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func pipeConn(t *testing.T) net.Conn {
	t.Helper()
	a, b := net.Pipe()
	t.Cleanup(func() {
		_ = a.Close()
		_ = b.Close()
	})
	return a
}

func TestRetryingDial_TwoFailuresThenSuccess(t *testing.T) {
	setDialAttemptTimeout(t, 3*time.Second)

	var n atomic.Int64
	want := pipeConn(t)
	inner := func(ctx context.Context, network, addr string) (net.Conn, error) {
		if n.Add(1) < 3 {
			return nil, errors.New("dial fail")
		}
		return want, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := retryingDial(ctx, "tcp", "127.0.0.1:3306", inner)
	require.NoError(t, err)
	require.Equal(t, want, conn)
	require.Equal(t, int64(3), n.Load(), "two failures then success is exactly 3 attempts")
}

func TestRetryingDial_OuterCtxExpiresReturnsLastError(t *testing.T) {
	setDialAttemptTimeout(t, 3*time.Second)

	sentinel := errors.New("dial boom")
	var n atomic.Int64
	inner := func(ctx context.Context, network, addr string) (net.Conn, error) {
		n.Add(1)
		return nil, sentinel
	}

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	start := time.Now()
	conn, err := retryingDial(ctx, "tcp", "127.0.0.1:3306", inner)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, sentinel)
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"budget expiry must be classifiable as a context error")
	require.Nil(t, conn)
	require.GreaterOrEqual(t, n.Load(), int64(1))
	require.Less(t, elapsed, 200*time.Millisecond,
		"must return when the outer ctx expires, not hang on the 250ms sleep (took %s)", elapsed)
}

// TestRetryingDial_AttemptOutlivesBudgetReturnsLastError covers the exit
// taken when the attempt itself consumes the rest of the outer budget (a
// blackholed SYN that blocks until the attempt ctx dies) — the failure ends
// with the outer ctx already expired, so the loop returns before the
// fast-failure sleep is ever considered.
func TestRetryingDial_AttemptOutlivesBudgetReturnsLastError(t *testing.T) {
	setDialAttemptTimeout(t, 3*time.Second)

	sentinel := errors.New("dial blackhole")
	inner := func(ctx context.Context, network, addr string) (net.Conn, error) {
		<-ctx.Done()
		return nil, sentinel
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	conn, err := retryingDial(ctx, "tcp", "127.0.0.1:3306", inner)
	require.Nil(t, conn)
	require.ErrorIs(t, err, sentinel)
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"budget expiry must be classifiable as a context error")
}

func TestRetryingDial_InactiveSingleAttempt(t *testing.T) {
	var n atomic.Int64
	sentinel := errors.New("once")
	inner := func(ctx context.Context, network, addr string) (net.Conn, error) {
		n.Add(1)
		return nil, sentinel
	}

	conn, err := wrapDial(inner, false)(context.Background(), "tcp", "127.0.0.1:3306")
	require.ErrorIs(t, err, sentinel)
	require.Nil(t, conn)
	require.Equal(t, int64(1), n.Load(), "retry inactive must do exactly one attempt")
}

func TestRetryingDial_CancelDuringFastFailureSleep(t *testing.T) {
	setDialAttemptTimeout(t, 3*time.Second)

	sentinel := errors.New("connection refused")
	entered := make(chan struct{})
	inner := func(ctx context.Context, network, addr string) (net.Conn, error) {
		select {
		case <-entered:
		default:
			close(entered)
		}
		return nil, sentinel
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		_, err := retryingDial(ctx, "tcp", "127.0.0.1:3306", inner)
		errCh <- err
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("inner dial never called")
	}
	// First attempt is a fast failure; the retry loop is now in the 250ms sleep.
	time.Sleep(10 * time.Millisecond)
	start := time.Now()
	cancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, sentinel, "cancel during the sleep must return the last dial error")
		require.ErrorIs(t, err, context.Canceled,
			"cancellation must be classifiable as a context error")
		require.Less(t, time.Since(start), 100*time.Millisecond,
			"cancel during the 250ms sleep must return promptly (took %s)", time.Since(start))
	case <-time.After(time.Second):
		t.Fatal("retryingDial hung after ctx cancel during the fast-failure sleep")
	}
}
