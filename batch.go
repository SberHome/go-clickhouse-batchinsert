package batchinsert

import (
	"container/list"
	"database/sql"
	"errors"
	"sync"
	"time"
)

var ErrClosed = errors.New("batch insert is closed")

type options struct {
	// Enable debug output
	debug bool
	//
	logger Logger
	//
	maxBatchSize int
	//
	flushPeriod time.Duration
}

var defaultOptions = options{
	logger:       nopLogger{},
	maxBatchSize: 10000,
	flushPeriod:  1 * time.Second,
}

type Option func(opt *options)

// WithDebug enable set debug output
func WithDebug(debug bool) Option {
	return func(opt *options) {
		opt.debug = debug
	}
}

// WithLogger set logger
func WithLogger(logger Logger) Option {
	return func(opt *options) {
		opt.logger = logger
	}
}

//
func WithMaxBatchSize(maxBatchSize int) Option {
	return func(opt *options) {
		opt.maxBatchSize = maxBatchSize
	}
}

//
func WithFlushPeriod(flushPeriod time.Duration) Option {
	return func(opt *options) {
		opt.flushPeriod = flushPeriod
	}
}

//
type BatchInsert struct {
	opts      options
	mu        sync.Mutex
	wg        sync.WaitGroup
	db        *sql.DB
	cache     *list.List
	kick      chan struct{}
	exit      chan struct{}
	insertSql string
	exited    bool
}

//
func New(db *sql.DB, insertSql string, opts ...Option) (*BatchInsert, error) {
	//
	options := defaultOptions
	for _, opt := range opts {
		opt(&options)
	}
	//
	b := &BatchInsert{
		opts:      options,
		db:        db,
		cache:     list.New(),
		kick:      make(chan struct{}),
		exit:      make(chan struct{}),
		insertSql: insertSql,
	}
	b.wg.Add(1)
	go func() {
		b.flusher()
		b.wg.Done()
	}()
	return b, nil
}

//
func (b *BatchInsert) Insert(values ...interface{}) (err error) {
	b.mu.Lock()

	if b.exited {
		b.mu.Unlock()
		return ErrClosed
	}

	b.cache.PushBack(values)
	if b.cache.Len() >= b.opts.maxBatchSize {
		select {
		case b.kick <- struct{}{}:
		default:
		}
	}

	b.mu.Unlock()
	return
}

func (b *BatchInsert) Close() {
	b.mu.Lock()
	if b.exited {
		b.mu.Unlock()
		return
	}
	b.exited = true
	close(b.exit)
	b.mu.Unlock()
	b.wg.Wait()
	return
}

//
func (b *BatchInsert) flusher() {
	ticker := time.NewTicker(b.opts.flushPeriod)
	defer ticker.Stop()
	exited := false
	for {
		select {
		case <-ticker.C:
		case <-b.kick:
		case <-b.exit:
			exited = true
		}
		b.mu.Lock()
		l := b.cache
		b.cache = list.New()
		b.mu.Unlock()
		startTime := time.Now()
		if l.Len() > 0 {
			err := batchInsert(b.db, b.insertSql, l)
			if err != nil {
				b.opts.logger.Log("flush error", err)
			} else {
				b.opts.logger.Log("flushed", l.Len(), "cost", time.Since(startTime))
			}
		}
		if exited {
			close(b.kick)
			b.opts.logger.Log("flusher", "stop")
			return
		}
	}
}

//
func batchInsert(db *sql.DB, insertSql string, values *list.List) (err error) {
	var tx *sql.Tx
	tx, err = db.Begin()
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	stmt, err := tx.Prepare(insertSql)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for e := values.Front(); e != nil; e = e.Next() {
		_, err = stmt.Exec(e.Value.([]interface{})...)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (b *BatchInsert) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cache.Len()
}
