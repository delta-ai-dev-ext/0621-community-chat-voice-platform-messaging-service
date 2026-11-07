// Package repository holds the PostgreSQL persistence layer. Every query is
// parameterized; repositories never build SQL by string concatenation.
package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Querier is the subset of pgx used by repositories. Keeping it narrow lets the
// persistence layer be exercised in unit tests with fake rows instead of a live
// database, while *pgxpool.Pool satisfies it directly.
type Querier interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Transactor opens transactions; *pgxpool.Pool satisfies it.
type Transactor interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// TransactionManager runs a function inside a single PostgreSQL transaction.
// Services depend on this interface so the send path can be tested without a
// database.
type TransactionManager interface {
	InTx(ctx context.Context, fn func(ctx context.Context) error) error
}

type txContextKey struct{}

// TxManager is the pgx-backed TransactionManager. Transactions are carried in
// the context, so repository calls made inside fn automatically join the open
// transaction without threading an extra parameter through every service.
type TxManager struct {
	pool Transactor
}

// NewTxManager builds a TransactionManager over a connection pool.
func NewTxManager(pool Transactor) *TxManager { return &TxManager{pool: pool} }

// InTx begins a transaction, runs fn with a transaction-scoped context and
// commits. A nested call joins the outer transaction instead of opening a new
// one, which keeps composite service operations atomic.
func (m *TxManager) InTx(ctx context.Context, fn func(ctx context.Context) error) error {
	if fn == nil {
		return errors.New("repository: nil transaction function")
	}
	if TxFromContext(ctx) != nil {
		return fn(ctx)
	}
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("repository: beginning transaction: %w", err)
	}
	txCtx := context.WithValue(ctx, txContextKey{}, tx)
	if err := fn(txCtx); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			return fmt.Errorf("%w (rollback failed: %v)", err, rbErr)
		}
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("repository: committing transaction: %w", err)
	}
	return nil
}

// TxFromContext returns the transaction carried by ctx, or nil.
func TxFromContext(ctx context.Context) pgx.Tx {
	tx, _ := ctx.Value(txContextKey{}).(pgx.Tx)
	return tx
}

// base embeds the fallback querier and resolves the effective one per call.
type base struct {
	db Querier
}

// q returns the transaction from the context when the caller is inside InTx,
// otherwise the pool. This is the only place transaction routing happens.
func (b base) q(ctx context.Context) Querier {
	if tx := TxFromContext(ctx); tx != nil {
		return tx
	}
	return b.db
}
