package mysql

import "database/sql/driver"

type Valueser interface {
	MySQLValues() ([]driver.Value, error)
}
