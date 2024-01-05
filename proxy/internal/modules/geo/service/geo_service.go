package service

import (
	"fmt"
	geogrpc "github.com/Bubotka/Microservices/proxy/internal/infrastructure/clients/geo"

	"github.com/Bubotka/Microservices/proxy/internal/models"
)

type GeoService struct {
	geoProvider geogrpc.GeoProviderer
}

func NewGeoService(geoProvider geogrpc.GeoProviderer) *GeoService {
	return &GeoService{geoProvider: geoProvider}
}

func (g *GeoService) ListLevenshtein(in ListlIn) ListlOut {
	levenshtein, err := g.geoProvider.ListLevenshtein(in.Column, in.Text)
	if err != nil {
		return ListlOut{models.SearchHistoryAddress{}, err}
	}
	return ListlOut{levenshtein, nil}
}

func (g *GeoService) Create(in CreateIn) {
	g.geoProvider.Create(in.SHA)
}

func (g *GeoService) Search(in SearchIn) SearchOut {
	fmt.Println("Зашли в Search")
	addressReworked, err := g.geoProvider.AddressSearch(in.Data)
	if err != nil {
		return SearchOut{}
	}
	return SearchOut{
		Addresses: addressReworked,
		Error:     nil,
	}
}

func (g *GeoService) Geo(in GeoIn) GeoOut {
	addressReworked, err := g.geoProvider.GeoCode(in.Data)
	if err != nil {
		return GeoOut{}
	}
	return GeoOut{
		Addresses: addressReworked,
		Error:     nil,
	}
}
