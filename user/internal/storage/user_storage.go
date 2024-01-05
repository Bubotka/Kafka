package storage

import (
	"context"
	"fmt"
	"github.com/Bubotka/Microservices/geo/pkg/db/adapter"
	"github.com/Bubotka/Microservices/user/domain/models"
)

type UserStorage struct {
	adapter adapter.SqlAdapterer
}

func (u *UserStorage) CheckUser(ctx context.Context, user models.User) error {
	var users []models.User
	_ = u.adapter.List(ctx, &user, models.User{}, adapter.Condition{
		Equal: map[string]interface{}{
			"email":    user.Email,
			"password": user.Password,
		},
	})

	if len(users) == 0 {
		return nil
	}

	return fmt.Errorf("такой пользователь уже существует")
}

func NewUserStorage(adapter adapter.SqlAdapterer) *UserStorage {
	return &UserStorage{adapter: adapter}
}

func (u *UserStorage) Create(ctx context.Context, user models.User) error {
	err := u.adapter.Create(ctx, user)

	return err
}

func (u *UserStorage) GetByEmail(ctx context.Context, email string) (models.User, error) {
	var user []models.User
	err := u.adapter.List(ctx, &user, models.User{}, adapter.Condition{
		Equal: map[string]interface{}{
			"email": email,
		},
	})

	if len(user) == 0 {
		return models.User{}, fmt.Errorf("no such a user")
	}
	fmt.Println(user)

	return user[0], err
}

func (u *UserStorage) List(ctx context.Context, c adapter.Condition) ([]models.User, error) {
	var users []models.User
	err := u.adapter.List(ctx, &users, models.User{}, c)
	fmt.Println(err)
	if err != nil {
		return nil, err
	}
	return users, nil
}
