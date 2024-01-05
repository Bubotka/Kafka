package main

import (
	"fmt"
	"github.com/Bubotka/Microservices/user/domain/models"

	"github.com/Bubotka/Microservices/user/internal/grpc"

	"github.com/Bubotka/Microservices/geo/pkg/db"
	"github.com/Bubotka/Microservices/geo/pkg/db/adapter"
	"github.com/Bubotka/Microservices/geo/pkg/db/tools/migrator"
	"github.com/Masterminds/squirrel"
	_ "github.com/lib/pq"
	"os"
)

func main() {
	postgresDB, err := db.NewPostgresDB(db.Config{
		Host:     os.Getenv("DB_HOST"),
		Port:     os.Getenv("DB_PORT"),
		Username: os.Getenv("DB_USER"),
		Password: os.Getenv("DB_PASSWORD"),
		DBName:   os.Getenv("DB_NAME"),
	})

	if err != nil {
		fmt.Println("Не получилось подключиться к бд")
	}

	var generator migrator.SQLiteGenerator
	m := migrator.NewMigrator(postgresDB, &generator)
	err = m.Migrate(&models.User{})
	if err != nil {
		fmt.Println("Не удалось мигрировать")
	}

	sqlAdapter := adapter.NewSQLAdapter(postgresDB, squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar))

	grpcServer := grpc.NewGrpcServer(sqlAdapter)
	grpcServer.Listen(":8083")
}
