package grpc

import (
	"github.com/Bubotka/Microservices/proxy/pkg/clients/user/grpc/client_adapter"
	"github.com/Bubotka/Microservices/user/domain/models"
)

//go:generate go run github.com/vektra/mockery/v2@v2.36.0 --name=UserProviderer
type UserProviderer interface {
	Create(user models.User) error
	CheckUser(user models.User) error
	Profile(email string) (models.User, error)
	List() ([]models.User, error)
}

type UserProvider struct {
	client client_adapter.UserClientAdapter
}

func NewUserProvider(client client_adapter.UserClientAdapter) *UserProvider {
	return &UserProvider{client: client}
}

func (u *UserProvider) CheckUser(user models.User) error { return u.client.CheckUser(user) }

func (u *UserProvider) Create(user models.User) error {
	return u.client.Create(user)
}

func (u *UserProvider) Profile(email string) (models.User, error) {
	return u.client.Profile(email)
}

func (u *UserProvider) List() ([]models.User, error) {
	return u.client.List()
}
