package mysql_test

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	mysql "github.com/StirlingMarketingGroup/cool-mysql"
	"github.com/davecgh/go-spew/spew"
	"github.com/jmoiron/sqlx"
	"github.com/shopspring/decimal"
)

func Benchmark_Genome_Cool_Select_Chan_NotCached(b *testing.B) {
	db, err := mysql.New(user, pass, schema, host, port,
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

	var genomeCh chan genomeRow
	for n := 0; n < b.N; n++ {
		genomeCh = make(chan genomeRow)
		err := db.Select(genomeCh, "select`upid`,`assembly_acc`,`assembly_version`,`total_length`,`created`from`genome`where`total_length`>@@TotalLength limit 1000", 0, mysql.Params{
			"TotalLength": 28111,
		})
		if err != nil {
			panic(err)
		}
	}
}

func Benchmark_Genome_Cool_Select_Slice_NotCached(b *testing.B) {
	db, err := mysql.New(user, pass, schema, host, port,
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

	var genomes []genomeRow
	for n := 0; n < b.N; n++ {
		genomes = genomes[:0]
		err := db.Select(&genomes, "select`upid`,`assembly_acc`,`assembly_version`,`total_length`,`created`from`genome`where`total_length`>@@TotalLength limit 1000", 0, mysql.Params{
			"TotalLength": 28111,
		})
		if err != nil {
			panic(err)
		}
	}

	// d, _ := json.Marshal(genomes)
	// fmt.Println("Benchmark_Genome_Cool_Select_Slice_NotCached", sha3.Sum224(d))
}

func Benchmark_Genome_Cool_Select_Struct_NotCached(b *testing.B) {
	db, err := mysql.New(user, pass, schema, host, port,
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
	for n := 0; n < b.N; n++ {
		err := db.Select(&genome, "select`upid`,`assembly_acc`,`assembly_version`,`total_length`,`created`from`genome`where`total_length`>@@TotalLength limit 1", 0, mysql.Params{
			"TotalLength": 28111,
		})
		if err != nil {
			panic(err)
		}
	}

	f, err := ioutil.TempFile("", "prefix")
	if err != nil {
		log.Fatal(err)
	}
	defer os.Remove(f.Name())
	spew.Fdump(f, len(genome.UpID))
}

func Benchmark_Genome_MySQL_Select_NotCached(b *testing.B) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&collation=utf8mb4_unicode_ci&parseTime=true",
		user,
		pass,
		host,
		port,
		schema,
	))
	if err != nil {
		panic(err)
	}

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	type genomeRow struct {
		UpID            string
		AssemblyAcc     sql.NullString
		AssemblyVersion sql.NullInt32
		TotalLength     decimal.Decimal
		Created         time.Time
	}

	for n := 0; n < b.N; n++ {
		rows, err := db.Query("select`upid`,`assembly_acc`,`assembly_version`,`total_length`,`created`from`genome`where`total_length`>? limit 1000", 28111)
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		for rows.Next() {
			var genome genomeRow
			err = rows.Scan(
				&genome.UpID,
				&genome.AssemblyAcc,
				&genome.AssemblyVersion,
				&genome.TotalLength,
				&genome.Created,
			)
			if err != nil {
				panic(err)
			}
		}
	}
}

func Benchmark_Genome_SQLx_Select_Slice_NotCached(b *testing.B) {
	db, err := sqlx.Connect("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&collation=utf8mb4_unicode_ci&parseTime=true",
		user,
		pass,
		host,
		port,
		schema,
	))
	if err != nil {
		panic(err)
	}

	type genomeRow struct {
		UpID            string          `db:"upid"`
		AssemblyAcc     sql.NullString  `db:"assembly_acc"`
		AssemblyVersion sql.NullInt32   `db:"assembly_version"`
		TotalLength     decimal.Decimal `db:"total_length"`
		Created         time.Time       `db:"created"`
	}

	var genomes []genomeRow
	for n := 0; n < b.N; n++ {
		genomes = genomes[:0]
		err := db.Select(&genomes, "select`upid`,`assembly_acc`,`assembly_version`,`total_length`,`created`from`genome`where`total_length`>? limit 1000", 28111)
		if err != nil {
			panic(err)
		}
	}

	// d, _ := json.Marshal(genomes)
	// fmt.Println("Benchmark_Genome_SQLx_Select_Slice_NotCached", sha3.Sum224(d))
}
