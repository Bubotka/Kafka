package migrator

import (
	"fmt"
	"github.com/Bubotka/Microservices/geo/domain/models"

	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"
)

type Migratorer interface {
	Migrate(tables ...func(tabler models.Tabler)) error
}

type Migrator struct {
	db           *sqlx.DB
	sqlGenerator SQLGenerator
}

func NewMigrator(db *sqlx.DB, sqlGenerator SQLGenerator) *Migrator {
	return &Migrator{
		db:           db,
		sqlGenerator: sqlGenerator,
	}
}

func (m *Migrator) Migrate(tables ...models.Tabler) error {
	var errGroup errgroup.Group
	for _, table := range tables {
		createSQL := m.sqlGenerator.CreateTableSQL(table)
		errGroup.Go(func() error {
			fmt.Println(createSQL)
			_, err := m.db.Exec(createSQL)
			return err
		})
	}

	return errGroup.Wait()
}
