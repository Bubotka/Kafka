package auth

import "net/http"

type AddressGeo struct {
	Suggestions []Suggestion `json:"suggestions"`
}

type Suggestion struct {
	Value             string            `json:"value"`
	UnrestrictedValue string            `json:"unrestricted_value"`
	Data              map[string]string `json:"data"`
}

type AddressSearch []AddressElement

type AddressElement struct {
	Result string `json:"result"`
	GeoLat string `json:"geo_lat"`
	GeoLon string `json:"geo_lon"`
}

type SearchRequest struct {
	Query string `json:"query"`
}

type GeocodeRequest struct {
	Lat string `json:"lat"`
	Lng string `json:"lng"`
}

type Client struct {
	Client *http.Client
	Url    string
	Method string
}

func NewClient(url string, method string) *Client {
	return &Client{Client: &http.Client{}, Url: url, Method: method}
}
