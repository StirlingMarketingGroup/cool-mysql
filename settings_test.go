package mysql

import (
	"database/sql"
	"time"

	"github.com/shopspring/decimal"
)

const user = "rfamro"
const pass = ""
const host = "mysql-rfam-public.ebi.ac.uk"
const port = 4497
const schema = "Rfam"

const cacheTime = 1

type genomeRow struct {
	UpID            string
	AssemblyAcc     sql.NullString
	AssemblyVersion sql.NullInt32
	TotalLength     decimal.Decimal
	Created         time.Time
}
