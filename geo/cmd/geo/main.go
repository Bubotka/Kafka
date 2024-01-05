package main

import (
	"fmt"
	"github.com/Bubotka/Microservices/geo/domain/models"
	"github.com/Bubotka/Microservices/geo/internal/cache"
	"github.com/Bubotka/Microservices/geo/internal/grpc"
	"github.com/Bubotka/Microservices/geo/pkg/db"
	"github.com/Bubotka/Microservices/geo/pkg/db/adapter"
	"github.com/Bubotka/Microservices/geo/pkg/db/tools/Initializer"
	"github.com/Bubotka/Microservices/geo/pkg/db/tools/migrator"
	"github.com/Masterminds/squirrel"
	"github.com/go-redis/redis"

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

	initializer := Initializer.NewInitializer(postgresDB)
	initializer.Init()

	var generator migrator.SQLiteGenerator
	m := migrator.NewMigrator(postgresDB, &generator)
	err = m.Migrate(&models.SearchHistoryAddress{})
	if err != nil {
		fmt.Println("Не удалось мигрировать")
	}

	client := redis.NewClient(&redis.Options{
		Addr: "redisgeo:6379",
	})

	redisCache := cache.NewRedis(client)
	sqlAdapter := adapter.NewSQLAdapter(postgresDB, squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar))

	grpcServer := grpc.NewGrpcServer(sqlAdapter, redisCache)
	grpcServer.Listen(":8081")
}
