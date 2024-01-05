package service

import (
	"github.com/Bubotka/Microservices/proxy/internal/infrastructure/clients/auth"
	"github.com/Bubotka/Microservices/user/domain/models"
)

type AuthService struct {
	auth auth.AuthProviderer
}

func NewAuthService(auth auth.AuthProviderer) *AuthService {
	return &AuthService{auth: auth}
}

func (a *AuthService) Register(in RegisterIn) RegisterOut {
	err := a.auth.Register(models.User{
		Email:    in.Email,
		Password: in.Password,
	})
	return RegisterOut{err}
}
func (a *AuthService) Login(in LoginIn) LoginOut {
	token, err := a.auth.Login(models.User{
		Email:    in.Email,
		Password: in.Password,
	})
	if err != nil {
		return LoginOut{"", err}
	}
	return LoginOut{Token: token, Error: err}
}
