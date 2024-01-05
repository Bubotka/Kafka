package test

import (
	"context"
	"github.com/Bubotka/Microservices/geo/pkg/db/adapter"
	"github.com/Bubotka/Microservices/user/domain/models"
	"github.com/Bubotka/Microservices/user/internal/service"
	"github.com/Bubotka/Microservices/user/internal/storage/mocks"
	us "github.com/Bubotka/Microservices/user/pkg/go/user"
	"github.com/golang/protobuf/ptypes/empty"
	"reflect"
	"testing"
)

func TestUser_CheckUser(t *testing.T) {
	type args struct {
		ctx     context.Context
		request *us.UserRequest
	}
	tests := []struct {
		name    string
		args    args
		want    *empty.Empty
		wantErr bool
	}{
		{
			name: "base_test",
			args: args{
				ctx: context.Background(),
				request: &us.UserRequest{User: &us.User{
					Email:    "kolia",
					Password: "123",
				}},
			},
		},
	}
	userRepository := mocks.NewUserRepository(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := service.NewUserService(userRepository)

			userRepository.On("CheckUser", context.Background(), models.User{Email: "kolia", Password: "123"}).
				Return(nil)

			_, err := u.CheckUser(tt.args.ctx, tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckUser() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestUser_Create(t *testing.T) {
	type args struct {
		ctx     context.Context
		request *us.UserRequest
	}
	tests := []struct {
		name    string
		args    args
		want    *empty.Empty
		wantErr bool
	}{
		{
			name: "base_test",
			args: args{
				ctx: context.Background(),
				request: &us.UserRequest{User: &us.User{
					Email:    "kolia",
					Password: "123",
				}},
			},
		},
	}
	userRepository := mocks.NewUserRepository(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := service.NewUserService(userRepository)
			userRepository.On("Create", context.Background(), models.User{Email: "kolia", Password: "123"}).
				Return(nil)
			_, err := u.Create(tt.args.ctx, tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("Create() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestUser_List(t *testing.T) {

	type args struct {
		ctx   context.Context
		empty *empty.Empty
	}
	tests := []struct {
		name    string
		args    args
		want    *us.ListResponse
		wantErr bool
	}{
		{
			name: "base_test",
			args: args{
				ctx:   context.Background(),
				empty: &empty.Empty{},
			},
			want: &us.ListResponse{},
		},
	}
	userRepository := mocks.NewUserRepository(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := service.NewUserService(userRepository)
			userRepository.On("List", context.Background(), adapter.Condition{}).Return([]models.User{}, nil)
			got, err := u.List(tt.args.ctx, tt.args.empty)
			if (err != nil) != tt.wantErr {
				t.Errorf("List() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("List() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUser_Profile(t *testing.T) {
	type args struct {
		ctx     context.Context
		request *us.ProfileRequest
	}
	tests := []struct {
		name    string
		args    args
		want    *us.ProfileResponse
		wantErr bool
	}{
		{
			name: "base_test",
			args: args{
				ctx:     context.Background(),
				request: &us.ProfileRequest{Email: "kolia"},
			},
			want: &us.ProfileResponse{},
		},
	}
	userRepository := mocks.NewUserRepository(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := service.NewUserService(userRepository)
			userRepository.On("GetByEmail", context.Background(), "kolia").Return(models.User{}, nil)
			_, err := u.Profile(tt.args.ctx, tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("Profile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
