package batchinsert_test

import (
	"database/sql"
	"net/url"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	batchinsert "github.com/sberhome/go-clickhouse-batchinsert"
	"github.com/stretchr/testify/require"

	_ "github.com/ClickHouse/clickhouse-go"
)

const testHost = "127.0.0.1:9000"

func GetUrl() string {
	u := new(url.URL)
	u.Scheme = "tcp"
	u.Host = testHost

	v := url.Values{}
	v.Set("write_timeout", strconv.Itoa(1))
	u.RawQuery = v.Encode()
	return u.String()
}

func Test(t *testing.T) {

	db, err := sql.Open("clickhouse", GetUrl())
	require.NoError(t, err)
	defer func() {
		require.NoError(t, db.Close())
	}()

	err = db.Ping()
	require.NoError(t, err)
	//
	testTable := `
			CREATE TABLE IF NOT EXISTS test (
				x        UInt8
			) engine=Memory`
	testSql := "INSERT INTO test (x) values (?)"
	//
	b, err := batchinsert.New(
		db,
		testSql,
		batchinsert.WithDebug(false),
		batchinsert.WithFlushPeriod(time.Second),
		batchinsert.WithMaxBatchSize(1000000),
	)
	require.NoError(t, err)
	//
	_, err = db.Exec(testTable)
	require.NoError(t, err)
	//
	t.Log("NumCPU", runtime.NumCPU())
	var wg sync.WaitGroup
	var count int64
	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := 0
			for {
				err := b.Insert(i)
				if err != nil {
					require.EqualError(t, err, batchinsert.ErrClosed.Error())
					atomic.AddInt64(&count, int64(i))
					return
				}
				i++
			}
		}()
	}
	//
	time.Sleep(5 * time.Second)
	b.Close()

	require.Equal(t, 0, b.Len())
	wg.Wait()
	//
	require.Equal(t, count, int64(selectCount(t, db, "test")))
	_, err = db.Exec(`DROP TABLE IF EXISTS test`)
	require.NoError(t, err)
	//
	t.Logf("insert count %d\n", count)
}

//
func selectCount(t *testing.T, db *sql.DB, table string) int {
	rows, err := db.Query("SELECT count() FROM " + table)
	require.NoError(t, err)
	defer rows.Close()

	for rows.Next() {
		var count int
		require.NoError(t, rows.Scan(&count))
		return count
	}
	return -1
}
