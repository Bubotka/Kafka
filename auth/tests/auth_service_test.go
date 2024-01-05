package tests

import (
	"context"
	"fmt"
	"github.com/Bubotka/Microservices/auth/internal/services/auth"
	au "github.com/Bubotka/Microservices/auth/pkg/go/auth"
	"github.com/Bubotka/Microservices/proxy/pkg/clients/user/grpc/mocks"
	"github.com/Bubotka/Microservices/user/domain/models"
	"github.com/golang/protobuf/ptypes/empty"
	"testing"
)

func TestAuthService_Login(t *testing.T) {
	type args struct {
		ctx     context.Context
		request *au.UserAuthRequest
	}
	tests := []struct {
		name    string
		args    args
		want    *au.LoginResponse
		wantErr bool
	}{
		{
			name: "base_test",
			args: args{
				ctx: context.Background(),
				request: &au.UserAuthRequest{
					Username: "kolia",
					Password: "123",
				},
			},
			wantErr: true,
		},
	}
	userProviderer := mocks.NewUserProviderer(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := auth.NewAuthService(userProviderer)

			userProviderer.On("Profile", "kolia").Return(models.User{
				Username: "kolia",
				Password: "123",
			}, nil)

			_, err := a.Login(tt.args.ctx, tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("Login() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestAuthService_Register(t *testing.T) {
	type args struct {
		ctx     context.Context
		request *au.UserAuthRequest
	}
	tests := []struct {
		name string

		args    args
		want    *empty.Empty
		wantErr bool
	}{
		{
			name: "base_test",
			args: args{
				ctx: context.Background(),
				request: &au.UserAuthRequest{
					Username: "kolia",
					Password: "",
				},
			},
			wantErr: true,
		},
	}
	userProviderer := mocks.NewUserProviderer(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := auth.NewAuthService(userProviderer)

			userProviderer.On("CheckUser", models.User{Username: "kolia", Password: ""}).Return(fmt.Errorf("exist"))

			_, err := a.Register(tt.args.ctx, tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("Register() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
