package mysql

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	smgMySQL "github.com/StirlingMarketingGroup/go-smg/mysql"
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

func BenchmarkSMGSelectIsCached(b *testing.B) {
	db, err := smgMySQL.NewDataBase(
		fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&collation=utf8mb4_unicode_ci",
			user,
			pass,
			host,
			port,
			schema,
		),
		fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&collation=utf8mb4_unicode_ci",
			user,
			pass,
			host,
			port,
			schema,
		),
	)
	if err != nil {
		panic(err)
	}

	type genomeRow struct {
		UpID            string
		AssemblyAcc     string
		AssemblyVersion int
		TotalLength     decimal.Decimal
		Created         time.Time
	}

	var genome genomeRow

	var i int
	for n := 0; n < b.N; n++ {
		d, err := db.Get("select`upid`,`assembly_acc`,`assembly_version`,`total_length`,`created`from`genome`where`total_length`>@@TotalLength limit 1000", cacheTime, smgMySQL.Params{
			"TotalLength": 28111,
		})
		if err != nil {
			panic(err)
		}
		for _, r := range d {
			genome.UpID = smgMySQL.Str(r, "upid")
			genome.AssemblyAcc = smgMySQL.Str(r, "assembly_acc")
			genome.AssemblyVersion = smgMySQL.Int(r, "assembly_version")
			genome.TotalLength = smgMySQL.Decimal(r, "total_length")
			genome.Created = smgMySQL.Time(r, "created", smgMySQL.Date)

			i++
		}
	}

	fmt.Println(i, genome)
}
