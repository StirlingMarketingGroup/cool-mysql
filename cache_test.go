package mysql

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func BenchmarkCoolSelectChanIsCached(b *testing.B) {
	db, err := New(user, pass, schema, host, port,
		user, pass, schema, host, port,
		nil)
	if err != nil {
		panic(err)
	}

	type genomeRow struct {
		UpID            string          `mysql:"upid"`
		AssemblyAcc     sql.NullString  `mysql:"assembly_acc"`
		AssemblyVersion sql.NullInt32   `mysql:"assembly_version"`
		TotalLength     decimal.Decimal `mysql:"total_length"`
		Created         time.Time       `mysql:"created"`
	}

	var genome genomeRow

	var genomeCh chan genomeRow
	var i int
	for n := 0; n < b.N; n++ {
		genomeCh = make(chan genomeRow)
		err := db.Select(genomeCh, "select`upid`,`assembly_acc`,`assembly_version`,`total_length`,`created`from`genome`where`total_length`>@@TotalLength limit 1000", cacheTime, Params{
			"TotalLength": 28111,
		})
		if err != nil {
			panic(err)
		}

		for r := range genomeCh {
			genome = r

			i++
		}
	}

	fmt.Println(i, genome)
}
