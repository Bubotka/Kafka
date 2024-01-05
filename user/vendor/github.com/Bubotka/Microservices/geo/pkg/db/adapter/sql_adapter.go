package adapter

import (
	"context"
	"fmt"
	"github.com/Bubotka/Microservices/geo/domain/models"

	sq "github.com/Masterminds/squirrel"
	"github.com/jmoiron/sqlx"
)

//go:generate go run github.com/vektra/mockery/v2@v2.36.0 --name=SqlAdapterer
type SqlAdapterer interface {
	Create(ctx context.Context, entity models.Tabler) error
	Update(ctx context.Context, entity models.Tabler, condition Condition, opts ...interface{}) error
	GetCount(ctx context.Context, entity models.Tabler, condition Condition) (int, error)
	List(ctx context.Context, dest interface{}, table models.Tabler, condition Condition) error
	ListLevenshtein(ctx context.Context, dest interface{}, table models.Tabler, columnName, targetText string) error
}

type SQLAdapter struct {
	db         *sqlx.DB
	sqlBuilder sq.StatementBuilderType
}

func NewSQLAdapter(db *sqlx.DB, sqlBuilder sq.StatementBuilderType) *SQLAdapter {
	return &SQLAdapter{db: db, sqlBuilder: sqlBuilder}
}

func (s *SQLAdapter) buildLevenshteinQuery(tableName, columnName, targetText string) (string, []interface{}) {
	queryBuilder := sq.StatementBuilder.
		Select("*").
		From(tableName).
		Where(fmt.Sprintf("levenshtein(%s, '%s') <= LENGTH('%s') * 0.3", columnName, targetText, targetText))

	query, args, _ := queryBuilder.ToSql()

	return query, args
}

func (s *SQLAdapter) buildSelect(tableName string, condition Condition, fields ...string) (string, []interface{}, error) {
	if condition.ForUpdate {
		temp := []string{"FOR UPDATE"}
		temp = append(temp, fields...)
		fields = temp
	}

	var queryRaw sq.SelectBuilder

	if len(fields) > 0 {
		queryRaw = s.sqlBuilder.Select(fields...).From(tableName)
	} else {
		queryRaw = s.sqlBuilder.Select("*").From(tableName)
	}

	if condition.Equal != nil {
		for field, args := range condition.Equal {
			queryRaw = queryRaw.Where(sq.Eq{field: args})
		}
	}

	if condition.NotEqual != nil {
		for field, args := range condition.NotEqual {
			queryRaw = queryRaw.Where(sq.NotEq{field: args})
		}
	}

	if condition.Order != nil {
		for _, order := range condition.Order {
			direction := "DESC"
			if order.Asc {
				direction = "ASC"
			}
			queryRaw = queryRaw.OrderBy(fmt.Sprintf("%s %s", order.Field, direction))
		}
	}

	if condition.LimitOffset != nil {
		if condition.LimitOffset.Limit > 0 {
			queryRaw = queryRaw.Limit(uint64(condition.LimitOffset.Limit))
		}
		if condition.LimitOffset.Offset > 0 {
			queryRaw = queryRaw.Offset(uint64(condition.LimitOffset.Offset))
		}
	}
	return queryRaw.ToSql()
}

func (s *SQLAdapter) Create(ctx context.Context, entity models.Tabler) error {
	insertBuilder := s.sqlBuilder.Insert(entity.TableName())

	columns := GetStructInfo(entity)

	sql, args, err := insertBuilder.Columns(columns.Fields[1:]...).Values(columns.Values[1:]...).ToSql()
	s.db.ExecContext(ctx, sql, args...)
	return err
}

func (s *SQLAdapter) GetCount(ctx context.Context, entity models.Tabler, condition Condition) (int, error) {
	query, args, err := s.buildSelect(entity.TableName(), condition, "COUNT(*)")
	if err != nil {
		return 0, err
	}
	var count int
	err = s.db.QueryRowxContext(ctx, query, args...).Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (s *SQLAdapter) List(ctx context.Context, dest interface{}, table models.Tabler, condition Condition) error {
	request, args, err := s.buildSelect(table.TableName(), condition)
	if err != nil {
		return err
	}

	err = s.db.SelectContext(ctx, dest, request, args...)

	return err
}

func (s *SQLAdapter) ListLevenshtein(ctx context.Context, dest interface{}, table models.Tabler, columnName, targetText string) error {
	request, args := s.buildLevenshteinQuery(table.TableName(), columnName, targetText)
	err := s.db.SelectContext(ctx, dest, request, args...)

	return err
}

func (s *SQLAdapter) Update(ctx context.Context, entity models.Tabler, condition Condition, opts ...interface{}) error {
	fields := make(map[string]interface{})
	for _, opt := range opts {
		if f, ok := opt.(map[string]interface{}); ok {
			for k, v := range f {
				fields[k] = v
			}
		}
	}

	request, args, err := s.sqlBuilder.Update(entity.TableName()).SetMap(fields).Where(condition.Equal).ToSql()

	fmt.Println(request)

	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, request, args...)
	if err != nil {
		return err
	}
	return err
}
