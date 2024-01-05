package db

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"time"
)

type Config struct {
	Host     string
	Port     string
	Username string
	Password string
	DBName   string
}

func NewPostgresDB(cfg Config) (*sqlx.DB, error) {
	var db *sqlx.DB
	var err error
	for i := 0; i < 5; i++ {
		time.Sleep(2 * time.Second)
		fmt.Println(cfg)
		db, err = sqlx.Connect("postgres", fmt.Sprintf("user=%s password=%s host=%s port=%s dbname=%s sslmode=disable",
			cfg.Username, cfg.Password, cfg.Host, cfg.Port, cfg.DBName))

		if err != nil {
			fmt.Println("Не удалось подключиться к бд")
			continue
		}

		err = db.Ping()
		if err != nil {
			fmt.Println("Не удалось пингануть бд")
			continue
		}

		fmt.Println("Успешное подключение")
		return db, err
	}
	return nil, err
}
