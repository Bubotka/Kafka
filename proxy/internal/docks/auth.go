package docks

//swagger:route Post /api/auth/register auth registerRequest
// Регистрация пользователя.
// Responses:
//   200: registerResponse

//swagger:parameters registerRequest
type registerRequest struct {
	// Userdata - данные пользователя
	// in: body
	// required: true
	// example: {"email":"asdas@gmail.com","password":"123"}
	Userdata string
}

// swagger:response registerResponse
type registerResponse struct {
	// in: body
	// Result резултат операции.
	// example: {"Register is succeed"}
	Result string
}

//swagger:route Post /api/auth/login auth loginRequest
// Авторизация пользователя.
// Responses:
//   200: loginResponse

//swagger:parameters loginRequest
type loginRequest struct {
	// Userdata - данные пользователя
	// in: body
	// required: true
	// example: {"email":"asdas@gmail.com","password":"123"}
	Userdata string
}

// swagger:response loginResponse
type loginResponse struct {
	// in: body
	// Token токен пользователя.
	Token string
}
