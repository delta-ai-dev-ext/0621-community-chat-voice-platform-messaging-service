// Package mocks holds the test doubles used across the service's unit and
// integration tests. Nothing here is compiled into the binary.
package mocks

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// NewTag builds a command tag reporting n affected rows.
func NewTag(n int64) pgconn.CommandTag {
	return pgconn.NewCommandTag(fmt.Sprintf("UPDATE %d", n))
}

// FakeRow is a scripted pgx.Row. A nil value slice means "no rows", which makes
// the fake return pgx.ErrNoRows — the signal repositories translate into a
// domain-meaningful outcome.
type FakeRow struct {
	Values []any
	Err    error
}

// Scan implements pgx.Row.
func (r *FakeRow) Scan(dest ...any) error {
	if r.Err != nil {
		return r.Err
	}
	if r.Values == nil {
		return pgx.ErrNoRows
	}
	return Assign(dest, r.Values)
}

// NewRow builds a row with the given column values.
func NewRow(values ...any) *FakeRow { return &FakeRow{Values: values} }

// NoRow builds a row that reports pgx.ErrNoRows.
func NoRow() *FakeRow { return &FakeRow{} }

// FakeRows is a scripted pgx.Rows over a slice of column-value tuples.
type FakeRows struct {
	rows    [][]any
	current []any
	index   int
	err     error
}

// NewRows builds result rows from column-value tuples.
func NewRows(rows ...[]any) *FakeRows { return &FakeRows{rows: rows} }

// NewErrorRows builds a result set that fails while iterating.
func NewErrorRows(err error) *FakeRows { return &FakeRows{err: err} }

// Close implements pgx.Rows.
func (r *FakeRows) Close() {}

// Err implements pgx.Rows.
func (r *FakeRows) Err() error { return r.err }

// CommandTag implements pgx.Rows.
func (r *FakeRows) CommandTag() pgconn.CommandTag { return NewTag(int64(len(r.rows))) }

// FieldDescriptions implements pgx.Rows.
func (r *FakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }

// Next implements pgx.Rows.
func (r *FakeRows) Next() bool {
	if r.err != nil || r.index >= len(r.rows) {
		return false
	}
	r.current = r.rows[r.index]
	r.index++
	return true
}

// Scan implements pgx.Rows.
func (r *FakeRows) Scan(dest ...any) error {
	if r.current == nil {
		return fmt.Errorf("mocks: Scan called before Next")
	}
	return Assign(dest, r.current)
}

// Values implements pgx.Rows.
func (r *FakeRows) Values() ([]any, error) { return r.current, nil }

// RawValues implements pgx.Rows.
func (r *FakeRows) RawValues() [][]byte { return nil }

// Conn implements pgx.Rows.
func (r *FakeRows) Conn() *pgx.Conn { return nil }

// TypeMap implements pgx.Rows.
func (r *FakeRows) TypeMap() *pgtype.Map { return nil }

// Call records one scripted database interaction.
type Call struct {
	SQL  string
	Args []any
}

// Script is a scripted Querier (and Transactor): queued responses are returned
// in call order and every call is recorded for assertions.
type Script struct {
	mu        sync.Mutex
	rows      []*FakeRows
	row       []*FakeRow
	execs     []pgconn.CommandTag
	errs      []error
	calls     []Call
	execCall  []Call
	beginErr  error
	commitErr error
}

// NewScript builds an empty scripted database.
func NewScript() *Script { return &Script{} }

// QueueRows enqueues a result set for the next Query call.
func (s *Script) QueueRows(rows *FakeRows) *Script {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rows = append(s.rows, rows)
	return s
}

// QueueRow enqueues a result for the next QueryRow call.
func (s *Script) QueueRow(row *FakeRow) *Script {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.row = append(s.row, row)
	return s
}

// QueueExec enqueues the command tag returned by the next Exec call.
func (s *Script) QueueExec(tag pgconn.CommandTag) *Script {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.execs = append(s.execs, tag)
	return s
}

// QueueError enqueues an error returned by the next call of any kind.
func (s *Script) QueueError(err error) *Script {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errs = append(s.errs, err)
	return s
}

// SetBeginError makes the next Begin call fail.
func (s *Script) SetBeginError(err error) *Script {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.beginErr = err
	return s
}

// SetCommitError makes the next Commit call fail.
func (s *Script) SetCommitError(err error) *Script {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commitErr = err
	return s
}

// Calls returns a copy of the recorded calls in order.
func (s *Script) Calls() []Call {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Call, len(s.calls))
	copy(out, s.calls)
	return out
}

// ExecCalls returns the recorded Exec calls only.
func (s *Script) ExecCalls() []Call {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Call, len(s.execCall))
	copy(out, s.execCall)
	return out
}

func (s *Script) popError() error {
	if len(s.errs) == 0 {
		return nil
	}
	err := s.errs[0]
	s.errs = s.errs[1:]
	return err
}

// Exec implements Querier.
func (s *Script) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, Call{SQL: sql, Args: args})
	s.execCall = append(s.execCall, Call{SQL: sql, Args: args})
	if err := s.popError(); err != nil {
		return pgconn.CommandTag{}, err
	}
	if len(s.execs) == 0 {
		return NewTag(1), nil
	}
	tag := s.execs[0]
	s.execs = s.execs[1:]
	return tag, nil
}

// Query implements Querier.
func (s *Script) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, Call{SQL: sql, Args: args})
	if err := s.popError(); err != nil {
		return nil, err
	}
	if len(s.rows) == 0 {
		return NewRows(), nil
	}
	rows := s.rows[0]
	s.rows = s.rows[1:]
	return rows, nil
}

// QueryRow implements Querier.
func (s *Script) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, Call{SQL: sql, Args: args})
	if err := s.popError(); err != nil {
		return &FakeRow{Err: err}
	}
	if len(s.row) == 0 {
		return NoRow()
	}
	row := s.row[0]
	s.row = s.row[1:]
	return row
}

// Begin implements Transactor.
func (s *Script) Begin(ctx context.Context) (pgx.Tx, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, Call{SQL: "BEGIN"})
	if s.beginErr != nil {
		return nil, s.beginErr
	}
	return &FakeTx{Script: s, ctx: ctx}, nil
}

// FakeTx is a scripted pgx.Tx that records commit and rollback outcomes.
type FakeTx struct {
	*Script
	ctx       context.Context
	Committed bool
	Rolled    bool
}

// Begin implements pgx.Tx.
func (t *FakeTx) Begin(ctx context.Context) (pgx.Tx, error) { return t.Script.Begin(ctx) }

// Commit implements pgx.Tx.
func (t *FakeTx) Commit(context.Context) error {
	t.Committed = true
	t.Script.mu.Lock()
	defer t.Script.mu.Unlock()
	if t.Script.commitErr != nil {
		return t.Script.commitErr
	}
	return nil
}

// Rollback implements pgx.Tx.
func (t *FakeTx) Rollback(context.Context) error {
	t.Rolled = true
	return nil
}

// CopyFrom implements pgx.Tx.
func (t *FakeTx) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return 0, nil
}

// SendBatch implements pgx.Tx.
func (t *FakeTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults { return nil }

// LargeObjects implements pgx.Tx.
func (t *FakeTx) LargeObjects() pgx.LargeObjects { return pgx.LargeObjects{} }

// Prepare implements pgx.Tx.
func (t *FakeTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, nil
}

// Exec implements pgx.Tx.
func (t *FakeTx) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return t.Script.Exec(ctx, sql, args...)
}

// Query implements pgx.Tx.
func (t *FakeTx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.Script.Query(ctx, sql, args...)
}

// QueryRow implements pgx.Tx.
func (t *FakeTx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return t.Script.QueryRow(ctx, sql, args...)
}

// Conn implements pgx.Tx.
func (t *FakeTx) Conn() *pgx.Conn { return nil }

// Assign copies driver values into scan destinations. It intentionally supports
// only the column types this service persists, so an unexpected column type
// fails loudly in tests instead of silently zero-valuing.
func Assign(dest []any, values []any) error {
	if len(dest) != len(values) {
		return fmt.Errorf("mocks: scan destination count %d does not match column count %d", len(dest), len(values))
	}
	for i, dst := range dest {
		if err := assignOne(dst, values[i]); err != nil {
			return fmt.Errorf("mocks: column %d: %w", i, err)
		}
	}
	return nil
}

func assignOne(dst, src any) error {
	switch target := dst.(type) {
	case *string:
		value, ok := src.(string)
		if !ok {
			return fmt.Errorf("cannot scan %T into *string", src)
		}
		*target = value
	case **string:
		if src == nil {
			*target = nil
			return nil
		}
		switch value := src.(type) {
		case string:
			*target = &value
		case *string:
			*target = value
		default:
			return fmt.Errorf("cannot scan %T into **string", src)
		}
	case *int:
		value, ok := src.(int)
		if !ok {
			return fmt.Errorf("cannot scan %T into *int", src)
		}
		*target = value
	case *int64:
		value, ok := src.(int64)
		if !ok {
			return fmt.Errorf("cannot scan %T into *int64", src)
		}
		*target = value
	case **int64:
		if src == nil {
			*target = nil
			return nil
		}
		switch value := src.(type) {
		case int64:
			*target = &value
		case *int64:
			*target = value
		default:
			return fmt.Errorf("cannot scan %T into **int64", src)
		}
	case *bool:
		value, ok := src.(bool)
		if !ok {
			return fmt.Errorf("cannot scan %T into *bool", src)
		}
		*target = value
	case *time.Time:
		value, ok := src.(time.Time)
		if !ok {
			return fmt.Errorf("cannot scan %T into *time.Time", src)
		}
		*target = value
	case **time.Time:
		if src == nil {
			*target = nil
			return nil
		}
		value, ok := src.(time.Time)
		if !ok {
			return fmt.Errorf("cannot scan %T into **time.Time", src)
		}
		*target = &value
	case *[]string:
		if src == nil {
			*target = nil
			return nil
		}
		value, ok := src.([]string)
		if !ok {
			return fmt.Errorf("cannot scan %T into *[]string", src)
		}
		*target = value
	case *[]byte:
		value, ok := src.([]byte)
		if !ok {
			return fmt.Errorf("cannot scan %T into *[]byte", src)
		}
		*target = value
	case *pgconn.CommandTag:
		value, ok := src.(pgconn.CommandTag)
		if !ok {
			return fmt.Errorf("cannot scan %T into *pgconn.CommandTag", src)
		}
		*target = value
	default:
		return assignReflect(dst, src)
	}
	return nil
}

func assignReflect(dst, src any) error {
	target := reflect.ValueOf(dst)
	if target.Kind() != reflect.Ptr || target.IsNil() {
		return fmt.Errorf("unsupported scan destination %T", dst)
	}
	value := target.Elem()
	if src == nil {
		value.Set(reflect.Zero(value.Type()))
		return nil
	}
	source := reflect.ValueOf(src)
	if source.Type().AssignableTo(value.Type()) {
		value.Set(source)
		return nil
	}
	if source.Type().ConvertibleTo(value.Type()) {
		value.Set(source.Convert(value.Type()))
		return nil
	}
	return fmt.Errorf("cannot scan %T into %T", src, dst)
}
