package client_adapter

import (
	"context"
	"fmt"
	"github.com/Bubotka/Microservices/user/domain/models"
	us "github.com/Bubotka/Microservices/user/pkg/go/user"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"log"
	"net"
	"time"
)

type UserClientGRpcAdapter struct {
	client us.UsererClient
}

func NewUserClientGRpcAdapter(client us.UsererClient) *UserClientGRpcAdapter {
	return &UserClientGRpcAdapter{client: client}
}

func (c *UserClientGRpcAdapter) Create(user models.User) error {
	req := &us.UserRequest{User: &us.User{
		Email:    user.Email,
		Password: user.Password,
	}}
	_, err := c.client.Create(context.Background(), req)
	return err
}

func (c *UserClientGRpcAdapter) CheckUser(user models.User) error {
	req := &us.UserRequest{User: &us.User{
		Email:    user.Email,
		Password: user.Password,
	}}
	_, err := c.client.CheckUser(context.Background(), req)
	return err
}

func (c *UserClientGRpcAdapter) Profile(username string) (models.User, error) {
	req := &us.ProfileRequest{Email: username}
	user, err := c.client.Profile(context.Background(), req)
	if err != nil {
		return models.User{}, err
	}

	return models.User{
		ID:       0,
		Email:    user.User.Email,
		Password: user.User.Password,
		IsDelete: false,
	}, nil
}

func (c *UserClientGRpcAdapter) List() ([]models.User, error) {
	response, err := c.client.List(context.Background(), &empty.Empty{})
	if err != nil {
		return nil, err
	}
	var users []models.User
	for _, r := range response.Users {
		user := models.User{
			ID:       0,
			Email:    r.Email,
			Password: r.Password,
			IsDelete: false,
		}
		users = append(users, user)
	}

	return users, err
}

func Connect(address string) (us.UsererClient, error) {
	for i := 0; i < 8; i++ {
		_, err := net.Dial("tcp", address)
		if err != nil {
			log.Println("Ошибка при подключении к серверу:", err)
			time.Sleep(3 * time.Second)
			continue
		}

		conn, err := grpc.Dial(address, grpc.WithInsecure())
		client := us.NewUsererClient(conn)
		log.Println("Клиент подключился по адресу: ", address)

		return client, nil
	}
	return nil, fmt.Errorf("unsuccessful connection")
}
