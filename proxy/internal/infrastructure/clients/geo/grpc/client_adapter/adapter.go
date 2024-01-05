package client_adapter

import "github.com/Bubotka/Microservices/proxy/internal/models"

type GeoClientAdapter interface {
	ListLevenshtein(column, text string) (models.SearchHistoryAddress, error)
	Create(sha models.SearchHistoryAddress) error
	Search(input []byte) (models.AddressSearchReworked, error)
	GeoCode(input []byte) (models.AddressSearchReworked, error)
}
