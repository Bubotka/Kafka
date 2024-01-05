package controller

import "github.com/Bubotka/Microservices/proxy/internal/models"

type SearchResponse struct {
	Addresses models.AddressSearchReworked `json:"addresses"`
}

type GeocodeResponse struct {
	Addresses models.AddressSearchReworked `json:"addresses"`
}

type SearchRequest struct {
	Place string `json:"query"`
}
