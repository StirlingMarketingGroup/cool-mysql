package mysql

// Die will dump the next query and then exit
func (db *Database) Die() {
	db.die = true
}
