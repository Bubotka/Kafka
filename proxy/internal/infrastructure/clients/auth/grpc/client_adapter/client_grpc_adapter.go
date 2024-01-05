package client_adapter

import (
	"context"
	"fmt"
	au "github.com/Bubotka/Microservices/auth/pkg/go/auth"
	"github.com/Bubotka/Microservices/user/domain/models"
	"google.golang.org/grpc"
	"log"
	"net"
	"time"
)

type AuthClientGRpcAdapter struct {
	client au.AuthClient
}

func NewAuthClientGRpcAdapter(client au.AuthClient) *AuthClientGRpcAdapter {
	return &AuthClientGRpcAdapter{client: client}
}

func (a *AuthClientGRpcAdapter) Register(user models.User) error {
	req := &au.UserAuthRequest{
		Email:    user.Email,
		Password: user.Password,
	}
	_, err := a.client.Register(context.Background(), req)
	return err
}

func (a *AuthClientGRpcAdapter) Login(user models.User) (string, error) {
	req := &au.UserAuthRequest{
		Email:    user.Email,
		Password: user.Password,
	}
	token, err := a.client.Login(context.Background(), req)
	if err != nil {
		return "", err
	}
	return token.Token, nil
}

func Connect(address string) (au.AuthClient, error) {
	for i := 0; i < 8; i++ {
		_, err := net.Dial("tcp", address)
		if err != nil {
			log.Println("Ошибка при подключении к серверу:", err)
			time.Sleep(3 * time.Second)
			continue
		}

		conn, err := grpc.Dial(address, grpc.WithInsecure())
		client := au.NewAuthClient(conn)
		log.Println("Клиент подключился по адресу: ", address)

		return client, nil
	}
	return nil, fmt.Errorf("unsuccessful connection")
}
