// Package database owns the PostgreSQL schema: embedded migrations plus the
// runner executed once at startup, before the service accepts traffic.
package database

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"

	"github.com/golang-migrate/migrate/v4"
	migratepgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

// migrationDriverName is the name golang-migrate uses for its pgx/v5 driver.
const migrationDriverName = "pgx5"

// Migrate applies every pending SQL migration. It is idempotent: running it on
// an up-to-date database returns nil (migrate.ErrNoChange is not an error).
func Migrate(ctx context.Context, databaseURL string) error {
	if databaseURL == "" {
		return errors.New("database: DATABASE_URL is required")
	}
	driver, err := newMigrate(databaseURL)
	if err != nil {
		return err
	}
	defer func() { drainAndClose(driver) }()

	if err := driver.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("database: applying migrations: %w", err)
	}
	return nil
}

// Version reports the currently applied migration version and whether the
// schema is in a dirty state.
func Version(ctx context.Context, databaseURL string) (uint, bool, error) {
	driver, err := newMigrate(databaseURL)
	if err != nil {
		return 0, false, err
	}
	defer func() { drainAndClose(driver) }()
	version, dirty, err := driver.Version()
	if err != nil {
		return 0, false, fmt.Errorf("database: reading migration version: %w", err)
	}
	return version, dirty, nil
}

func newMigrate(databaseURL string) (*migrate.Migrate, error) {
	sub, err := fs.Sub(migrationFiles, "migrations")
	if err != nil {
		return nil, fmt.Errorf("database: opening embedded migrations: %w", err)
	}
	source, err := iofs.New(sub, ".")
	if err != nil {
		return nil, fmt.Errorf("database: building migration source: %w", err)
	}
	db, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return nil, fmt.Errorf("database: opening connection pool: %w", err)
	}
	databaseDriver, err := migratepgx.WithInstance(db, &migratepgx.Config{})
	if err != nil {
		// WithInstance pings the database, so a failure here means the pool was
		// unusable; release it instead of leaking the handle.
		_ = db.Close()
		return nil, fmt.Errorf("database: building migration driver: %w", err)
	}
	driver, err := migrate.NewWithInstance("iofs", source, migrationDriverName, databaseDriver)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("database: building migrator: %w", err)
	}
	return driver, nil
}

// drainAndClose closes the migrator. The error from Close is intentionally
// ignored because both the source and database drivers only release already
// used resources here.
func drainAndClose(m *migrate.Migrate) {
	sourceErr, databaseErr := m.Close()
	_ = sourceErr
	_ = databaseErr
}
