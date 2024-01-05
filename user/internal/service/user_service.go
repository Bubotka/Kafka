package service

import (
	"context"
	"fmt"
	"github.com/Bubotka/Microservices/geo/pkg/db/adapter"
	"github.com/Bubotka/Microservices/user/domain/models"
	"github.com/Bubotka/Microservices/user/internal/storage"
	us "github.com/Bubotka/Microservices/user/pkg/go/user"
	"github.com/golang/protobuf/ptypes/empty"
)

type User struct {
	storage storage.UserRepository
	us.UnimplementedUsererServer
}

func NewUserService(storage storage.UserRepository) *User {
	return &User{storage: storage}
}

func (u *User) CheckUser(ctx context.Context, request *us.UserRequest) (*empty.Empty, error) {
	err := u.storage.CheckUser(ctx, models.User{
		Email:    request.User.Email,
		Password: request.User.Password,
	})
	return &empty.Empty{}, err
}

func (u *User) Profile(ctx context.Context, request *us.ProfileRequest) (*us.ProfileResponse, error) {
	fmt.Println("Profile")
	user, err := u.storage.GetByEmail(ctx, request.Email)
	if err != nil {
		return nil, err
	}
	return &us.ProfileResponse{User: &us.User{
		Email:    user.Email,
		Password: user.Password,
	}}, nil
}

func (u *User) List(ctx context.Context, empty *empty.Empty) (*us.ListResponse, error) {
	fmt.Println("List")
	respone, err := u.storage.List(ctx, adapter.Condition{})
	if err != nil {
		return nil, err
	}

	var users []*us.User
	for _, r := range respone {
		user := &us.User{
			Email:    r.Email,
			Password: r.Password,
		}
		users = append(users, user)
	}

	return &us.ListResponse{Users: users}, nil
}

func (u *User) Create(ctx context.Context, request *us.UserRequest) (*empty.Empty, error) {
	user := models.User{
		ID:       0,
		Email:    request.User.Email,
		Password: request.User.Password,
		IsDelete: false,
	}

	err := u.storage.Create(ctx, user)
	return &empty.Empty{}, err
}
