# go-clickhouse-batchinsert
go client (batch insert) base on database/sql module
### Clickhouse client for batch insert
Ref to https://clickhouse.yandex/docs/en/introduction/performance/

`We recommend inserting data in packets of at least 1000 rows, or no more than a single request per second. `

We better insert data in bulk, clickhouse-client can do bulk insert like this
```go
    var (
      tx, _   = connect.Begin()
      stmt, _ = tx.Prepare("...")
    )
    defer stmt.Close()

    for _, obj := objs {
      if _, err := stmt.Exec(...); err != nil {
        log.Fatal(err)
      }
    }

    if err := tx.Commit(); err != nil {
      log.Fatal(err)
    }
```
So we only need to cache objs, and flush them at a fixed time or a fixed amount.

#### Example
```go
        
import (
	"database/sql"
	batchinsert "github.com/sberhome/go-clickhouse-batchinsert"
	
	_ "github.com/ClickHouse/clickhouse-go"
)

func ExampleBatchInsert(url string) {
	// connect to DB
	db, err := sql.Open("clickhouse", url)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		_ = db.Close()
		return nil, err
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
		batchinsert.WithHost("127.0.0.1:9000"),
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
	b.Insert(1)
	b.Insert(2)
	b.Insert(3)
	//
	_, err = db.Exec(`DROP TABLE IF EXISTS test`)
	if err != nil {
		panic(err)
	}
	err = b.Close()
	if err != nil {
		panic(err)
	}

	// Output:
}
```
