package migrator

import (
	"fmt"
	"github.com/Bubotka/Microservices/geo/domain/models"

	"reflect"
	"strings"
)

type SQLGenerator interface {
	CreateTableSQL(table models.Tabler) string
}

type SQLiteGenerator struct{}

func (sg *SQLiteGenerator) CreateTableSQL(table models.Tabler) string {
	tableName := table.TableName()
	request := fmt.Sprintf("CREATE TABLE if NOT EXISTS %v (", tableName)
	structType := reflect.TypeOf(table).Elem()
	var result []string

	for i := 0; i < structType.NumField(); i++ {
		field := structType.Field(i)
		db := field.Tag.Get("db")
		dbType := field.Tag.Get("db_type")

		result = append(result, fmt.Sprintf("%v %v", db, dbType))
	}
	request += strings.Join(result, ", ") + ");"

	return request
}
