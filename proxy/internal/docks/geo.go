package docks

import (
	"github.com/Bubotka/Microservices/proxy/internal/models"
)

//swagger:route Post /api/address/geocode geo geoRequest
// Определение адреса на основе широты и долготы.
// security:
// 	- Bearer: []
// Responses:
//   200: geolocationResponse

//swagger:parameters geoRequest
type geoRequest struct {
	// Coordinates - кординаты на карте
	// in: body
	// required: true
	// example: {"lat":"59.948474778247544","lng":"30.296516418457035"}
	Coordinates string
}

// swagger:response geolocationResponse
type geolocationResponse struct {
	// in: body
	// Addresses содержит информацию об определенном географическом адресе.
	Addresses models.AddressSearchReworked
}

// swagger:route POST /api/address/search geo searchRequest
// Поиск адресов на основе запроса адреса.
// security:
// 	- Bearer: []
// Responses:
//   200: searchResponse

// swagger:parameters searchRequest
type searchRequest struct {
	// Query - поисковой запрос адреса
	// in: body
	// required: true
	// example: {"query": "Москва"}
	Query string
}

// swagger:response searchResponse
type searchResponse struct {
	// in: body
	// Addresses содержит информацию о найденных адресах.
	Addresses models.AddressSearchReworked
}
