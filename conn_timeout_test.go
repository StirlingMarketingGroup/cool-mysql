package mysql

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mysqldrv "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
)

// This file exercises the real go-sql-driver wire protocol against a tiny
// in-process fake MySQL server so we can prove the #172 behavior end to end:
//
//   - a connection that goes silent on a read surfaces as mysql.ErrInvalidConn
//     within ReadTimeout instead of hanging until the caller's deadline,
//   - cool-mysql's existing retry then swaps to a fresh connection and the
//     query succeeds, and
//   - ReadTimeout is per-read, so a result set delivered in steady packets
//     with gaps shorter than ReadTimeout is unaffected even when its total
//     runtime exceeds ReadTimeout.
//
// The fake server accepts any credentials — it never validates the auth
// scramble, it just frames packets correctly and answers commands.

// --- minimal packet framing -------------------------------------------------

type fakeConn struct {
	net.Conn
}

func (fc *fakeConn) readPacket() (seq byte, payload []byte, err error) {
	header := make([]byte, 4)
	if _, err = io.ReadFull(fc.Conn, header); err != nil {
		return 0, nil, err
	}
	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	seq = header[3]
	payload = make([]byte, length)
	if _, err = io.ReadFull(fc.Conn, payload); err != nil {
		return seq, nil, err
	}
	return seq, payload, nil
}

func (fc *fakeConn) writePacket(seq byte, payload []byte) error {
	n := len(payload)
	header := []byte{byte(n), byte(n >> 8), byte(n >> 16), seq}
	if _, err := fc.Conn.Write(header); err != nil {
		return err
	}
	_, err := fc.Conn.Write(payload)
	return err
}

func lenEncStr(s string) []byte {
	// All strings in these tests are < 251 bytes, so the length-encoded
	// integer is a single byte.
	return append([]byte{byte(len(s))}, s...)
}

// okPacket is a protocol-41 OK packet: status = autocommit, no warnings.
var okPacket = []byte{0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00}

// eofPacket is a protocol-41 EOF packet (the server does not advertise
// CLIENT_DEPRECATE_EOF, so result sets are terminated by these).
var eofPacket = []byte{0xfe, 0x00, 0x00, 0x02, 0x00}

// handshakeInit is the server greeting (protocol version 10). Capability
// flags advertise CLIENT_PROTOCOL_41 + CLIENT_SECURE_CONNECTION +
// CLIENT_PLUGIN_AUTH with the mysql_native_password plugin, which lets the
// driver complete auth in a single round trip that we then accept with OK.
func handshakeInit() []byte {
	p := []byte{10} // protocol version
	p = append(p, "5.7.0-cool-fake"...)
	p = append(p, 0)                   // server version NUL
	p = append(p, 1, 0, 0, 0)          // connection id
	scramble := make([]byte, 20)       // auth-plugin-data (content ignored)
	p = append(p, scramble[:8]...)     // auth-plugin-data part 1
	p = append(p, 0)                   // filler
	p = append(p, 0x0f, 0xa2)          // capability flags (lower 16) = 0xa20f
	p = append(p, 0x21)                // charset (utf8_general_ci)
	p = append(p, 0x02, 0x00)          // status flags
	p = append(p, 0x08, 0x00)          // capability flags (upper 16) = 0x0008
	p = append(p, 21)                  // length of auth-plugin-data
	p = append(p, make([]byte, 10)...) // reserved
	p = append(p, scramble[8:20]...)   // auth-plugin-data part 2 (12 bytes)
	p = append(p, 0)                   // NUL terminating part 2
	p = append(p, "mysql_native_password"...)
	p = append(p, 0)
	return p
}

// --- fake server ------------------------------------------------------------

type queryAction int

const (
	actionOK    queryAction = iota // answer with an OK packet
	actionStall                    // go silent — never answer this command
	actionRows                     // answer with a one-column, one-row result set
)

// fakeServer speaks just enough of the MySQL protocol to let go-sql-driver
// connect and run commands. onQuery decides what to do with each COM_QUERY
// text; COM_PING is always answered OK.
type fakeServer struct {
	ln      net.Listener
	onQuery func(q string) (action queryAction, rowValue string, perPacketDelay time.Duration)
	wg      sync.WaitGroup
}

func startFakeServer(t *testing.T, onQuery func(q string) (queryAction, string, time.Duration)) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	s := &fakeServer{ln: ln, onQuery: onQuery}
	s.wg.Add(1)
	go s.acceptLoop()
	t.Cleanup(func() {
		_ = ln.Close()
		s.wg.Wait()
	})
	return s
}

func (s *fakeServer) dsn() string {
	// loc=UTC keeps the driver's BeforeConnect SET time_zone deterministic;
	// the fake server answers it OK like any other SET.
	return fmt.Sprintf("u:p@tcp(%s)/db?parseTime=true&loc=UTC", s.ln.Addr().String())
}

func (s *fakeServer) acceptLoop() {
	defer s.wg.Done()
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return // listener closed
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handle(&fakeConn{Conn: c})
		}()
	}
}

func (s *fakeServer) handle(fc *fakeConn) {
	defer fc.Conn.Close()

	// Greeting (seq 0) -> client handshake response (seq 1) -> OK (seq 2).
	if err := fc.writePacket(0, handshakeInit()); err != nil {
		return
	}
	if _, _, err := fc.readPacket(); err != nil {
		return
	}
	if err := fc.writePacket(2, okPacket); err != nil {
		return
	}

	for {
		seq, payload, err := fc.readPacket()
		if err != nil || len(payload) == 0 {
			return
		}
		cmd := payload[0]
		switch cmd {
		case 0x01: // COM_QUIT
			return
		case 0x0e: // COM_PING
			if err := fc.writePacket(seq+1, okPacket); err != nil {
				return
			}
		case 0x03: // COM_QUERY
			q := string(payload[1:])
			action, rowValue, delay := actionOK, "", time.Duration(0)
			if s.onQuery != nil {
				action, rowValue, delay = s.onQuery(q)
			}
			switch action {
			case actionStall:
				// Stay silent and keep the socket open: loop back to a
				// blocking read. The driver's ReadTimeout fires, it closes
				// the conn, and our read then returns an error so we exit.
				continue
			case actionRows:
				if err := s.writeResultSet(fc, seq+1, rowValue, delay); err != nil {
					return
				}
			default:
				if err := fc.writePacket(seq+1, okPacket); err != nil {
					return
				}
			}
		default:
			// Anything else we don't model: answer OK so the driver keeps
			// going (e.g. COM_STMT_* are never used by this library's path).
			if err := fc.writePacket(seq+1, okPacket); err != nil {
				return
			}
		}
	}
}

// writeResultSet sends a one-column ("v" VAR_STRING) one-row text result set,
// sleeping perPacketDelay before each packet so the total exceeds ReadTimeout
// while every individual gap stays under it.
func (s *fakeServer) writeResultSet(fc *fakeConn, startSeq byte, rowValue string, perPacketDelay time.Duration) error {
	sleep := func() {
		if perPacketDelay > 0 {
			time.Sleep(perPacketDelay)
		}
	}

	// 1) column count = 1
	sleep()
	if err := fc.writePacket(startSeq, []byte{0x01}); err != nil {
		return err
	}

	// 2) column definition (ColumnDefinition41)
	colDef := []byte{}
	colDef = append(colDef, lenEncStr("def")...)    // catalog
	colDef = append(colDef, lenEncStr("")...)       // schema
	colDef = append(colDef, lenEncStr("")...)       // table
	colDef = append(colDef, lenEncStr("")...)       // org_table
	colDef = append(colDef, lenEncStr("v")...)      // name
	colDef = append(colDef, lenEncStr("")...)       // org_name
	colDef = append(colDef, 0x0c)                   // length of fixed fields
	colDef = append(colDef, 0x21, 0x00)             // charset (utf8_general_ci)
	colDef = append(colDef, 0x00, 0x04, 0x00, 0x00) // column length
	colDef = append(colDef, 0xfd)                   // type = VAR_STRING
	colDef = append(colDef, 0x00, 0x00)             // flags
	colDef = append(colDef, 0x00)                   // decimals
	colDef = append(colDef, 0x00, 0x00)             // filler
	sleep()
	if err := fc.writePacket(startSeq+1, colDef); err != nil {
		return err
	}

	// 3) EOF terminating the column definitions
	sleep()
	if err := fc.writePacket(startSeq+2, eofPacket); err != nil {
		return err
	}

	// 4) one row (text protocol: length-encoded value)
	sleep()
	if err := fc.writePacket(startSeq+3, lenEncStr(rowValue)); err != nil {
		return err
	}

	// 5) EOF terminating the rows
	sleep()
	return fc.writePacket(startSeq+4, eofPacket)
}

// --- tests ------------------------------------------------------------------

const stallSentinel = "SELECT 'stall-me'"

// TestReadTimeout_HalfOpenSurfacesAsInvalidConn proves AC2: a connection that
// goes silent on a query read surfaces as mysql.ErrInvalidConn within
// ReadTimeout instead of blocking to the caller's context deadline. With
// MaxAttempts pinned to 1 the error is returned rather than retried, so we
// can assert on it directly.
func TestReadTimeout_HalfOpenSurfacesAsInvalidConn(t *testing.T) {
	setNetTimeouts(t, 250*time.Millisecond, 0, 0)

	origAttempts := MaxAttempts
	MaxAttempts = 1
	t.Cleanup(func() { MaxAttempts = origAttempts })

	srv := startFakeServer(t, func(q string) (queryAction, string, time.Duration) {
		// Match on the literal rather than the whole statement: a ctx with a
		// deadline makes SelectContext inject a MAX_EXECUTION_TIME hint, so the
		// query text the server sees is "SELECT /*+ ... */ 'stall-me'" (#174).
		if strings.Contains(q, "'stall-me'") {
			return actionStall, "", 0
		}
		return actionOK, "", 0
	})

	db, err := NewFromDSN(srv.dsn(), srv.dsn())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	// A generous context proves the failure is ReadTimeout (sub-second), not
	// the caller deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	var out []string
	err = db.SelectContext(ctx, &out, stallSentinel, 0)
	elapsed := time.Since(start)

	require.Error(t, err)
	require.True(t, errors.Is(err, mysqldrv.ErrInvalidConn),
		"a half-open read must surface as ErrInvalidConn, got %v", err)
	require.Less(t, elapsed, 5*time.Second,
		"the read must fail near ReadTimeout, not hang to the context deadline (took %s)", elapsed)
}

// TestReadTimeout_RetrySwapsConnAndSucceeds proves AC3: when the first
// attempt draws the half-open connection and times out, cool-mysql's retry
// swaps to a fresh connection and the query succeeds within the
// MaxExecutionTime budget — the timeout is never returned to the caller.
func TestReadTimeout_RetrySwapsConnAndSucceeds(t *testing.T) {
	setNetTimeouts(t, 250*time.Millisecond, 0, 0)

	// Only the *first* occurrence of the sentinel query stalls (the doomed
	// half-open conn); every later attempt returns a row, modeling recovery
	// on a fresh connection.
	var sentinelSeen atomic.Int64
	srv := startFakeServer(t, func(q string) (queryAction, string, time.Duration) {
		if strings.Contains(q, "'stall-me'") {
			if sentinelSeen.Add(1) == 1 {
				return actionStall, "", 0
			}
			return actionRows, "recovered", 0
		}
		return actionOK, "", 0
	})

	db, err := NewFromDSN(srv.dsn(), srv.dsn())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var out []string
	require.NoError(t, db.SelectContext(ctx, &out, stallSentinel, 0))
	require.Equal(t, []string{"recovered"}, out,
		"retry must run on a fresh connection and return the real result")
	require.GreaterOrEqual(t, sentinelSeen.Load(), int64(2),
		"the query must have been retried after the stalled attempt")
}

// TestReadTimeout_ExecRetriesOnFreshConnAfterStall covers the write path:
// ReadTimeout is applied to the writes pool too, so a half-open write conn
// surfaces mysql.ErrInvalidConn mid-Exec. exec must reconnect and re-run the
// statement on a fresh pooled conn rather than reporting the dead conn as a
// silent success — guards the exec.go ErrInvalidConn handling (it previously
// did `return nil, db.Test()`, which reports (nil, nil) as success).
func TestReadTimeout_ExecRetriesOnFreshConnAfterStall(t *testing.T) {
	setNetTimeouts(t, 250*time.Millisecond, 0, 0)

	const execStmt = "UPDATE t SET x = 1"
	var sentinelSeen atomic.Int64
	srv := startFakeServer(t, func(q string) (queryAction, string, time.Duration) {
		if q == execStmt {
			if sentinelSeen.Add(1) == 1 {
				return actionStall, "", 0 // first attempt draws the half-open conn
			}
			return actionOK, "", 0 // retry on a fresh conn succeeds
		}
		return actionOK, "", 0
	})

	db, err := NewFromDSN(srv.dsn(), srv.dsn())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	require.NoError(t, db.ExecContext(ctx, execStmt),
		"a half-open write conn must be retried on a fresh conn, not reported as success")
	require.GreaterOrEqual(t, sentinelSeen.Load(), int64(2),
		"the statement must actually re-run after the stalled attempt, not silently no-op")
}

// TestReadTimeout_HealthyStreamingQueryUnaffected proves AC4: ReadTimeout is
// applied per packet read, so a result set delivered in steady packets — each
// gap under ReadTimeout but the total well over it — completes normally.
func TestReadTimeout_HealthyStreamingQueryUnaffected(t *testing.T) {
	readTimeout := 500 * time.Millisecond
	setNetTimeouts(t, readTimeout, 0, 0)

	const streamQuery = "SELECT 'stream'"
	// The result set is 5 packets, so 5 * 150ms = ~750ms total runtime,
	// comfortably over the 500ms ReadTimeout — yet each per-packet gap
	// (150ms) has ~350ms of slack under it, so a loaded CI runner won't
	// spuriously trip the deadline between packets.
	perPacket := 150 * time.Millisecond
	srv := startFakeServer(t, func(q string) (queryAction, string, time.Duration) {
		if strings.Contains(q, "'stream'") {
			return actionRows, "streamed", perPacket
		}
		return actionOK, "", 0
	})

	db, err := NewFromDSN(srv.dsn(), srv.dsn())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	var out []string
	require.NoError(t, db.SelectContext(ctx, &out, streamQuery, 0))
	elapsed := time.Since(start)

	require.Equal(t, []string{"streamed"}, out)
	require.Greater(t, elapsed, readTimeout,
		"the query's total runtime must exceed ReadTimeout for this test to be meaningful (took %s)", elapsed)
}
