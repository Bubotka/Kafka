package client_adapter

import "github.com/Bubotka/Microservices/user/domain/models"

type AuthClientAdapter interface {
	Register(user models.User) error
	Login(user models.User) (string, error)
}
