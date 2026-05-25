package mysql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestMarshalTime_NaiveLocLiteral confirms time.Time values marshal as
// naive Loc-formatted datetime literals — no convert_tz, no session
// time zone. This is the read-symmetric form that lets go-sql-driver
// parse the value back through the same Loc and recover the original
// instant. See #157.
func TestMarshalTime_NaiveLocLiteral(t *testing.T) {
	// 2026-01-15T12:00:00Z is winter in America/New_York (EST, -05:00),
	// which is exactly the scenario the issue reproduces.
	utc := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)

	nyc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	got, err := marshal(utc, 0, "", nil, nyc)
	require.NoError(t, err)
	require.Equal(t, "'2026-01-15 07:00:00.000000'", string(got),
		"time.Time must be formatted as a naive datetime literal in loc, with no convert_tz wrapper")

	gotUTC, err := marshal(utc, 0, "", nil, time.UTC)
	require.NoError(t, err)
	require.Equal(t, "'2026-01-15 12:00:00.000000'", string(gotUTC))
}

// TestMarshalTime_DSTRoundTrip is the core repro from #157: writing a
// time.Time whose instant falls on the opposite side of a DST boundary
// from "now" must round-trip through the loc without slipping an hour.
//
// We simulate the round-trip end-to-end: marshal formats the literal
// using the loc; we strip the surrounding quotes and re-parse via
// time.ParseInLocation against the same loc (which is what go-sql-driver
// does internally on DATETIME columns). The parsed value must equal the
// original instant.
func TestMarshalTime_DSTRoundTrip(t *testing.T) {
	nyc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	const layout = "2006-01-02 15:04:05.000000"

	cases := []struct {
		name string
		in   time.Time
	}{
		// Winter instant (EST, -05:00).
		{name: "EST instant", in: time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)},
		// Summer instant (EDT, -04:00).
		{name: "EDT instant", in: time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)},
		// Right after spring-forward.
		{name: "just after spring-forward", in: time.Date(2026, 3, 8, 7, 30, 0, 0, time.UTC)},
		// Right before fall-back.
		{name: "just before fall-back", in: time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := marshal(tc.in, 0, "", nil, nyc)
			require.NoError(t, err)

			literal := string(got)
			// Strip the surrounding single quotes.
			require.True(t, len(literal) >= 2 && literal[0] == '\'' && literal[len(literal)-1] == '\'',
				"marshaled literal must be quoted: %q", literal)
			naive := literal[1 : len(literal)-1]

			// This is what the driver does on read: parse the naive
			// DATETIME against the connection's Loc.
			parsed, err := time.ParseInLocation(layout, naive, nyc)
			require.NoError(t, err)

			require.True(t, tc.in.Equal(parsed),
				"round-trip lost the instant: original=%s parsed=%s diff=%s",
				tc.in, parsed, parsed.Sub(tc.in))
		})
	}
}

// TestMarshalTime_ZeroIsNull guards the long-standing zero-value behavior
// — an unset time.Time emits SQL NULL rather than a meaningless literal.
func TestMarshalTime_ZeroIsNull(t *testing.T) {
	got, err := marshal(time.Time{}, 0, "", nil, time.UTC)
	require.NoError(t, err)
	require.Equal(t, "null", string(got))
}
