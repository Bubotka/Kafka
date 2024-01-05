package service

import "github.com/Bubotka/Microservices/user/domain/models"

type Userer interface {
	GetByEmail(in GetIn) GetOut
	List() ListOut
}

type CreateIn struct {
	User models.User
}

type CreateOut struct {
	Message string
	Error   error
}

type GetIn struct {
	Email string
}

type GetOut struct {
	User  models.User
	Error error
}

type ListOut struct {
	User  []models.User
	Error error
}
