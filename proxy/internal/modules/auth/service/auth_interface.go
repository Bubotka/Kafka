package service

type Auther interface {
	Register(in RegisterIn) RegisterOut
	Login(in LoginIn) LoginOut
}

type RegisterIn struct {
	Email    string
	Password string
}

type RegisterOut struct {
	Error error
}

type LoginIn struct {
	Email    string
	Password string
}
type LoginOut struct {
	Token string
	Error error
}
