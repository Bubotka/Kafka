package docks

import (
	"github.com/Bubotka/Microservices/proxy/internal/modules/user/controller"
	"github.com/Bubotka/Microservices/user/domain/models"
)

// swagger:route GET /api/user/list user ListRequest
// Все пользователи.
// security:
// 	- Bearer: []
// Responses:
//   200: ListResponseDock

// swagger:response ListResponseDock
type ListResponseDock struct {
	// in: body
	// Response ответ содержащий пользователей и их количество в бд.
	Response controller.ListResponse
}

// swagger:route GET /api/user/profile/{email} user ProfileRequest
// Инфа о пользователе.
// security:
// 	- Bearer: []
// Responses:
//   200: ProfileResponse

// swagger:parameters ProfileRequest
type ProfileRequest struct {
	// Email - имя пользователя
	// in: path
	// required: true
	Email string `json:"email"`
}

// swagger:response ProfileResponse
type ProfileResponse struct {
	// in: body
	// Response ответ содержащий пользователя.
	Response models.User
}
