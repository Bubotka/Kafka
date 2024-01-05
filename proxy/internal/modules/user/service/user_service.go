package service

import (
	"github.com/Bubotka/Microservices/proxy/pkg/clients/user/grpc"
	"github.com/Bubotka/Microservices/user/domain/models"
)

type UserService struct {
	user grpc.UserProviderer
}

func NewUserService(user grpc.UserProviderer) *UserService {
	return &UserService{user: user}
}

func (u *UserService) GetByEmail(in GetIn) GetOut {
	user, err := u.user.Profile(in.Email)
	if err != nil {
		return GetOut{models.User{}, err}
	}
	return GetOut{
		User:  user,
		Error: nil,
	}
}

func (u *UserService) List() ListOut {
	users, err := u.user.List()
	if err != nil {
		return ListOut{nil, err}
	}
	return ListOut{users, err}
}
