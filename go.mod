module github.com/BrianLeishman/cool-mysql

go 1.15

replace github.com/StirlingMarketingGroup/go-smg => ../../StirlingMarketingGroup/go-smg

require (
	github.com/StirlingMarketingGroup/go-smg v0.0.0-20201104155946-b3039ece9ee2
	github.com/davecgh/go-spew v1.1.1
	github.com/go-sql-driver/mysql v1.5.0
	github.com/jmoiron/sqlx v1.2.1-0.20200615141059-0794cb1f47ee
	github.com/pkg/errors v0.9.1
	github.com/shopspring/decimal v1.2.0
	golang.org/x/sys v0.0.0-20200223170610-d5e6a3e2c0ae // indirect
)
