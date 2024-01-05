package controller

import (
	"encoding/json"
	"github.com/Bubotka/Microservices/proxy/internal/infrastructure/responder"
	"github.com/Bubotka/Microservices/proxy/internal/modules/auth/service"

	"net/http"
)

type Auther interface {
	Register(w http.ResponseWriter, r *http.Request)
	Login(w http.ResponseWriter, r *http.Request)
}

type AuthController struct {
	auth service.Auther
	responder.Responder
}

func NewAuthController(auth service.Auther, responder responder.Responder) *AuthController {
	return &AuthController{auth: auth, Responder: responder}
}

func (a *AuthController) Register(w http.ResponseWriter, r *http.Request) {
	var user User
	err := json.NewDecoder(r.Body).Decode(&user)
	if err != nil {
		a.Responder.ErrorBadRequest(w, err)
		return
	}

	out := a.auth.Register(service.RegisterIn{
		Email:    user.Email,
		Password: user.Password,
	})

	if out.Error != nil {
		a.Responder.ErrorBadRequest(w, err)
		return
	}

	w.WriteHeader(http.StatusOK)
	a.OutputJSON(w, "Пользователь успешно зарегестрирован")
}

func (a *AuthController) Login(w http.ResponseWriter, r *http.Request) {
	var user User
	err := json.NewDecoder(r.Body).Decode(&user)
	if err != nil {
		a.Responder.ErrorBadRequest(w, err)
		return
	}

	out := a.auth.Login(service.LoginIn{
		Email:    user.Email,
		Password: user.Password,
	})

	if out.Error != nil {
		w.Write([]byte(out.Error.Error()))
		return
	}

	w.WriteHeader(http.StatusOK)
	a.OutputJSON(w, out.Token)
}
