package models

type AddressSearchReworked []AddressElementSearch

type AddressElementSearch struct {
	Result string `json:"result"`
	GeoLat string `json:"lat"`
	GeoLon string `json:"lon"`
}
