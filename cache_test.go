package mysql

import (
	"database/sql"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func Benchmark_Genome_Cool_Select_Chan_IsCached(b *testing.B) {
	db, err := New(user, pass, schema, host, port,
		user, pass, schema, host, port,
		nil)
	if err != nil {
		panic(err)
	}

	db.EnableRedis("localhost:6379", "", 0)

	type genomeRow struct {
		UpID            string          `mysql:"upid"`
		AssemblyAcc     sql.NullString  `mysql:"assembly_acc"`
		AssemblyVersion sql.NullInt32   `mysql:"assembly_version"`
		TotalLength     decimal.Decimal `mysql:"total_length"`
		Created         time.Time       `mysql:"created"`
	}

	var genomeCh chan genomeRow
	for n := 0; n < b.N; n++ {
		genomeCh = make(chan genomeRow)
		err := db.Select(genomeCh, "select`upid`,`assembly_acc`,`assembly_version`,`total_length`,`created`from`genome`where`total_length`>@@TotalLength limit 1000", time.Millisecond, Params{
			"TotalLength": 28111,
		})
		if err != nil {
			panic(err)
		}
	}
}
