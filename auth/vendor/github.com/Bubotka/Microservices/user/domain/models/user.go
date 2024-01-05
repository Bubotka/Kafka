package models

type User struct {
	ID       int    `db:"id" db_type:"SERIAL PRIMARY KEY"`
	Email    string `db:"email" db_type:"VARCHAR(100) UNIQUE"`
	Password string `db:"password" db_type:"VARCHAR(100)"`
	IsDelete bool   `db:"is_delete" db_type:"BOOLEAN"`
}

func (u User) TableName() string {
	return "users"
}
