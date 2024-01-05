package client_adapter

import "github.com/Bubotka/Microservices/user/domain/models"

type UserClientAdapter interface {
	Create(user models.User) error
	CheckUser(user models.User) error
	Profile(email string) (models.User, error)
	List() ([]models.User, error)
}
