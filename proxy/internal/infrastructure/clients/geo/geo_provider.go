package geo

import (
	"github.com/Bubotka/Microservices/proxy/internal/infrastructure/clients/geo/grpc/client_adapter"
	"github.com/Bubotka/Microservices/proxy/internal/models"
)

type GeoProviderer interface {
	ListLevenshtein(column, text string) (models.SearchHistoryAddress, error)
	Create(sha models.SearchHistoryAddress) error
	AddressSearch(input []byte) (models.AddressSearchReworked, error)
	GeoCode(input []byte) (models.AddressSearchReworked, error)
}

type GeoProvider struct {
	client client_adapter.GeoClientAdapter
}

func NewGeoProvider(client client_adapter.GeoClientAdapter) *GeoProvider {
	return &GeoProvider{client: client}
}

func (g *GeoProvider) ListLevenshtein(column, text string) (models.SearchHistoryAddress, error) {
	response, err := g.client.ListLevenshtein(column, text)
	if err != nil {
		return models.SearchHistoryAddress{}, err
	}
	return response, nil
}

func (g *GeoProvider) Create(sha models.SearchHistoryAddress) error {
	err := g.client.Create(sha)
	return err
}

func (g *GeoProvider) AddressSearch(input []byte) (models.AddressSearchReworked, error) {
	addressSearchReworked, err := g.client.Search(input)
	return addressSearchReworked, err
}

func (g *GeoProvider) GeoCode(input []byte) (models.AddressSearchReworked, error) {
	addressSearchReworked, err := g.client.GeoCode(input)
	return addressSearchReworked, err
}
