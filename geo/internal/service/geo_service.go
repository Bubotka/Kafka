package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Bubotka/Microservices/geo/domain/models"
	"github.com/Bubotka/Microservices/geo/internal/storage"
	"github.com/golang/protobuf/ptypes/empty"

	gp "github.com/Bubotka/Microservices/geo/pkg/go/geo"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

type GeoService struct {
	gp.UnimplementedGeoProviderServer
	storage storage.GeoRepository
}

func NewGeoService(storage storage.GeoRepository) *GeoService {
	return &GeoService{storage: storage}
}

func (g *GeoService) ListLevenshtein(ctx context.Context, req *gp.ListLevenshteinRequest) (*gp.ListLevenshteinResponse, error) {
	out, err := g.storage.ListLevenshtein(ctx, req.Column, req.Text)
	sha := &gp.SearchHistoryAddress{
		Id:              int32(out.Id),
		SearchRequest:   out.SearchRequest,
		AddressResponse: out.AddressResponse,
	}
	return &gp.ListLevenshteinResponse{Sha: sha}, err
}

func (g *GeoService) Create(ctx context.Context, req *gp.CreateRequest) (*empty.Empty, error) {
	sha := models.SearchHistoryAddress{
		Id:              int(req.Sha.Id),
		SearchRequest:   req.Sha.SearchRequest,
		AddressResponse: req.Sha.AddressResponse,
	}

	err := g.storage.Create(ctx, sha)
	return &empty.Empty{}, err
}

func (g *GeoService) Search(ctx context.Context, req *gp.SearchRequest) (*gp.SearchResponse, error) {
	fmt.Println("Зашли в search")
	dataForReq, err := makeDataForSearchReq([]byte(req.Place))
	if err != nil {
		return nil, err
	}

	client := NewClient("https://cleaner.dadata.ru/api/v1/clean/address", "POST")
	request, _ := MakeRequest(client, dataForReq)

	resp, err := client.Client.Do(request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	data, _ := ioutil.ReadAll(resp.Body)
	var search AddressSearch
	err = json.Unmarshal(data, &search)

	reworkedData := makeReworkedData(search[0].Result, search[0].GeoLat, search[0].GeoLon)

	var elements []*gp.AddressElementSearch
	for _, element := range reworkedData {
		elementSearch := gp.AddressElementSearch{
			Result: element.Result,
			GeoLat: element.GeoLat,
			GeoLon: element.GeoLon,
		}
		elements = append(elements, &elementSearch)
	}

	return &gp.SearchResponse{Elements: elements}, nil
}

func (g *GeoService) GeoCode(ctx context.Context, req *gp.GeoRequest) (*gp.GeoResponse, error) {
	value, err := makeDataForGeo([]byte(req.Coordinates))
	if err != nil {
		return nil, err
	}

	client := NewClient("https://suggestions.dadata.ru/suggestions/api/4_1/rs/geolocate/address", "POST")
	request, err := MakeRequest(client, value)

	resp, err := client.Client.Do(request)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respData, err := ioutil.ReadAll(resp.Body)
	search, err := suggestionToAddressGeo(respData)

	reworkedData := makeReworkedData(search[0].Result, search[0].GeoLat, search[0].GeoLon)
	var elements []*gp.AddressElementSearch
	for _, element := range reworkedData {
		elementSearch := gp.AddressElementSearch{
			Result: element.Result,
			GeoLat: element.GeoLat,
			GeoLon: element.GeoLon,
		}
		elements = append(elements, &elementSearch)
	}

	return &gp.GeoResponse{Elements: elements}, nil
}

func MakeRequest(client *Client, dataForReq string) (*http.Request, error) {
	req, err := http.NewRequest(client.Method, client.Url, strings.NewReader(dataForReq))

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Token 3d07e91f5dff46cc54fa1a829d7ac23ae1515b7b")
	req.Header.Set("X-Secret", "01dfd45ed227af41d55753c846ceb9834ac9084c")
	return req, err
}

func makeReworkedData(res, lat, lon string) models.AddressSearchReworked {
	latFloat, _ := strconv.ParseFloat(lat, 64)
	lonFloat, _ := strconv.ParseFloat(lon, 64)
	latStr := fmt.Sprintf("%.3f", latFloat)
	lonStr := fmt.Sprintf("%.3f", lonFloat)

	result := models.AddressSearchReworked{models.AddressElementSearch{
		Result: res,
		GeoLat: latStr,
		GeoLon: lonStr,
	}}
	return result
}

func makeDataForSearchReq(body []byte) (string, error) {
	var searchRequest SearchRequest
	err := json.Unmarshal(body, &searchRequest)
	if err != nil {
		fmt.Println("Failed to deserialize")
		return "", err
	}

	dataForReq := searchRequest.Query
	dataForReq = fmt.Sprintf("[ \"%s\" ]", dataForReq)
	return dataForReq, err
}

func makeDataForGeo(body []byte) (string, error) {
	var data GeocodeRequest
	err := json.Unmarshal(body, &data)
	if err != nil {
		return "", err
	}

	arg1, _ := strconv.ParseFloat(data.Lat, 64)
	arg2, _ := strconv.ParseFloat(data.Lng, 64)
	value := fmt.Sprintf("{ \"lat\": %.3f, \"lon\": %.3f }", arg1, arg2)
	return value, err
}

func suggestionToAddressGeo(respData []byte) (AddressSearch, error) {
	var suggestion AddressGeo
	json.Unmarshal(respData, &suggestion)

	if suggestion.Suggestions[0].Value == "" {
		return nil, fmt.Errorf("no values")
	}

	search := AddressSearch{AddressElement{
		Result: suggestion.Suggestions[0].Value,
		GeoLat: suggestion.Suggestions[0].Data["geo_lat"],
		GeoLon: suggestion.Suggestions[0].Data["geo_lon"],
	}}
	return search, nil
}
