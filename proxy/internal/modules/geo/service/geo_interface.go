package service

import (
	"github.com/Bubotka/Microservices/proxy/internal/models"
)

type GeoI interface {
	Create(in CreateIn)
	ListLevenshtein(in ListlIn) ListlOut
	Search(in SearchIn) SearchOut
	Geo(in GeoIn) GeoOut
}

type CreateIn struct {
	SHA models.SearchHistoryAddress
}

type ListlIn struct {
	Column string
	Text   string
}

type ListlOut struct {
	Sha   models.SearchHistoryAddress
	Error error
}

type SearchIn struct {
	Data []byte
}

type SearchOut struct {
	Addresses models.AddressSearchReworked
	Error     error
}

type GeoIn struct {
	Data []byte
}

type GeoOut struct {
	Addresses models.AddressSearchReworked
	Error     error
}
