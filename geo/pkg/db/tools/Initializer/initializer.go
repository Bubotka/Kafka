package Initializer

import (
	"github.com/jmoiron/sqlx"
)

type Initializer struct {
	db *sqlx.DB
}

func NewInitializer(db *sqlx.DB) *Initializer {
	return &Initializer{db: db}
}

func (i *Initializer) Init() {
	i.db.Exec("CREATE EXTENSION IF NOT EXISTS fuzzystrmatch")
}
