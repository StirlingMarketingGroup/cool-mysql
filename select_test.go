package mysql

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/StirlingMarketingGroup/go-smg/mysql"
	"github.com/jmoiron/sqlx"
)

func BenchmarkCoolSelect(b *testing.B) {
	db, err := New("rfamro", "", "Rfam", "mysql-rfam-public.ebi.ac.uk", 4497,
		"rfamro", "", "Rfam", "mysql-rfam-public.ebi.ac.uk", 4497,
		nil)
	if err != nil {
		panic(err)
	}

	type author struct {
		AuthorID int     `mysql:"author_id"`
		Name     string  `mysql:"name"`
		LastName *string `mysql:"last_name"`
		Initials *string `mysql:"initials"`
		Orcid    *string `mysql:"orcid"`
		Synonyms *string `mysql:"synonyms"`
	}

	var authorsCh chan author

	var authorID int
	var name string

	var i int
	for n := 0; n < b.N; n++ {
		authorsCh = make(chan author)
		err := db.Select(authorsCh, "select`author_id`,`name`,`last_name`,`initials`,`orcid`,`synonyms`from`author`", 0)
		if err != nil {
			panic(err)
		}

		for r := range authorsCh {
			authorID = r.AuthorID
			name = r.Name

			i++
		}
	}

	fmt.Println(i, authorID, name)
}

func BenchmarkSMGSelect(b *testing.B) {
	db, err := mysql.NewDataBase(
		fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&collation=utf8mb4_unicode_ci",
			"rfamro",
			"",
			"mysql-rfam-public.ebi.ac.uk",
			4497,
			"Rfam",
		),
		fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&collation=utf8mb4_unicode_ci",
			"rfamro",
			"",
			"mysql-rfam-public.ebi.ac.uk",
			4497,
			"Rfam",
		),
	)
	if err != nil {
		panic(err)
	}

	var authorID int
	var name string

	var i int
	for n := 0; n < b.N; n++ {
		d, err := db.Get("select`author_id`,`name`,`last_name`,`initials`,`orcid`,`synonyms`from`author`", 0)
		if err != nil {
			panic(err)
		}
		for _, r := range d {
			authorID = mysql.Int(r, "author_id")
			name = mysql.Str(r, "name")

			i++
		}
	}

	fmt.Println(i, authorID, name)
}

func BenchmarkMySQLSelect(b *testing.B) {
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&collation=utf8mb4_unicode_ci",
		"rfamro",
		"",
		"mysql-rfam-public.ebi.ac.uk",
		4497,
		"Rfam",
	))
	if err != nil {
		panic(err)
	}

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	var x interface{}

	var authorID int
	var name string

	var i int
	for n := 0; n < b.N; n++ {
		i = 0

		rows, err := db.Query("select`author_id`,`name`,`last_name`,`initials`,`orcid`,`synonyms`from`author`")
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		for rows.Next() {
			err = rows.Scan(&authorID, &name, &x, &x, &x, &x)

			i++
		}
	}

	fmt.Println(i, authorID, name)
}

func BenchmarkSQLxSelect(b *testing.B) {
	db, err := sqlx.Connect("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&collation=utf8mb4_unicode_ci",
		"rfamro",
		"",
		"mysql-rfam-public.ebi.ac.uk",
		4497,
		"Rfam",
	))
	if err != nil {
		panic(err)
	}

	type author struct {
		AuthorID int     `db:"author_id"`
		Name     string  `db:"name"`
		LastName *string `db:"last_name"`
		Initials *string `db:"initials"`
		Orcid    *string `db:"orcid"`
		Synonyms *string `db:"synonyms"`
	}

	var authorID int
	var name string

	authors := make([]author, 0, 100)

	var i int
	for n := 0; n < b.N; n++ {
		i = 0

		err := db.Select(&authors, "select`author_id`,`name`,`last_name`,`initials`,`orcid`,`synonyms`from`author`")
		if err != nil {
			panic(err)
		}

		for _, r := range authors {
			authorID = r.AuthorID
			name = r.Name

			i++
		}
	}

	fmt.Println(i, authorID, name)
}
