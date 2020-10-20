package mysql

import (
	"log"
	"testing"
)

func BenchmarkSelect(b *testing.B) {
	db, err := New("rfamro", "", "Rfam", "mysql-rfam-public.ebi.ac.uk", 4497,
		"rfamro", "", "Rfam", "mysql-rfam-public.ebi.ac.uk", 4497,
		nil)
	if err != nil {
		panic(err)
	}

	type author struct {
		AuthorID int    `mysql:"author_id"`
		Name     string `mysql:"name"`
		LastName string `mysql:"last_name"`
		Initials string `mysql:"initials"`
		Orcid    string `mysql:"orcid"`
		Synonyms string `mysql:"synonyms"`
	}

	authorsCh := make(chan author, 50)

	i := 0
	for n := 0; n < b.N; n++ {
		errCh := db.Select(authorsCh, "select`author_id`,`name`,`last_name`,`initials`,`orcid`,`synonyms`from`author`", 0)

		for {
			select {
			case <-authorsCh:
				i++
			case err := <-errCh:
				panic(err)
			}
		}
	}

	log.Println(len(authorsCh), i, "records found")
}
