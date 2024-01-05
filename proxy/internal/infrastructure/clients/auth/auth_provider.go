package auth

import (
	"github.com/Bubotka/Microservices/proxy/internal/infrastructure/clients/auth/grpc/client_adapter"
	"github.com/Bubotka/Microservices/user/domain/models"
)

type AuthProviderer interface {
	Register(user models.User) error
	Login(user models.User) (string, error)
}

type AuthProvider struct {
	client client_adapter.AuthClientAdapter
}

func NewAuthProvider(client client_adapter.AuthClientAdapter) *AuthProvider {
	return &AuthProvider{client: client}
}

func (g *AuthProvider) Register(user models.User) error {
	return g.client.Register(user)
}

func (g *AuthProvider) Login(user models.User) (string, error) {
	return g.client.Login(user)
}
