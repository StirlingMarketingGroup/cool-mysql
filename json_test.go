package mysql

import (
	"os"
	"testing"

	"github.com/StirlingMarketingGroup/go-smg/bid2"
	"github.com/davecgh/go-spew/spew"
)

func BenchmarkCoolSelectJSONChanNotCached(b *testing.B) {
	db, err := New(user, pass, schema, host, port,
		user, pass, schema, host, port,
		nil)
	if err != nil {
		panic(err)
	}

	type testRow struct {
		Ints    []int
		Strings []string
		Map     map[string][]string
		Bytes   []byte
		Base64  [][]byte
		Bid2s   map[bid2.String][]bid2.Bid2
	}

	var test testRow

	var testCh chan testRow
	var i int
	for n := 0; n < b.N; n++ {
		testCh = make(chan testRow)
		err := db.Select(testCh, "select'[1,2,3]'`Ints`,'[\"Swick\",\"Yeet\",\"swagswag\"]'`Strings`,"+
			"'{\"im a key\":[\"im a value\"]}'`Map`,random_bytes(8)`Bytes`,concat('[\"',to_base64(random_bytes(8)),'\"]')`Base64`,"+
			"'{\"FkXsNQIckkI\":[\"FkXsOgqbc_I\",\"FkXsPGWegGI\"]}'`Bid2s`", 0)
		if err != nil {
			panic(err)
		}

		for r := range testCh {
			test = r

			i++
		}
	}

	// fmt.Println(i, test)
	spew.Dump(i, test)
	os.Exit(0)
}
