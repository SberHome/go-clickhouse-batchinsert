package batchinsert_test

import (
	"database/sql"
	batchinsert "github.com/sberhome/go-clickhouse-batchinsert"

	_ "github.com/ClickHouse/clickhouse-go"
)

func ExampleBatchInsert() {
	// connect to DB
	db, err := sql.Open("clickhouse", "127.0.0.1:9000")
	if err != nil {
		return
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		_ = db.Close()
		return
	}
	//
	createTable := `
			CREATE TABLE IF NOT EXISTS test (
				x        UInt8
			) engine=Memory`
	insertSql := "INSERT INTO test (x) values (?)"
	//
	b, err := batchinsert.New(
		db,
		insertSql,
	)
	if err != nil {
		panic(err)
	}
	//
	_, err = db.Exec(createTable)
	if err != nil {
		panic(err)
	}
	//
	_ = b.Insert(1)
	_ = b.Insert(2)
	_ = b.Insert(3)
	//
	_, err = db.Exec(`DROP TABLE IF EXISTS test`)
	if err != nil {
		panic(err)
	}
	b.Close()
	if err != nil {
		panic(err)
	}

	// Output:
}
