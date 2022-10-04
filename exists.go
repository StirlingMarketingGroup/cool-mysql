package mysql

import (
	"context"
	"time"

	"golang.org/x/sync/errgroup"
)

// exists efficiently checks if there are any rows in the given query
func exists(db *Database, conn commander, ctx context.Context, q string, cache time.Duration, params ...any) (bool, error) {
	ch := make(chan struct{})
	grp := new(errgroup.Group)

	db = db.Clone()
	db.DisableUnusedColumnWarnings = true

	grp.Go(func() error {
		defer close(ch)
		return query(db, conn, ctx, ch, q, cache, params...)
	})

	exists := false

	for range ch {
		exists = true
	}

	if err := grp.Wait(); err != nil {
		return false, err
	}

	return exists, nil
}
