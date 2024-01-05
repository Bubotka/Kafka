package client_adapter

import (
	"context"
	"fmt"
	gp "github.com/Bubotka/Microservices/geo/pkg/go/geo"
	"github.com/Bubotka/Microservices/proxy/internal/models"
	"google.golang.org/grpc"
	"log"
	"net"
	"time"
)

type GeoClientGRpcAdapter struct {
	client gp.GeoProviderClient
}

func NewGeoClientGRpcAdapter(client gp.GeoProviderClient) *GeoClientGRpcAdapter {
	return &GeoClientGRpcAdapter{client: client}
}

func (c *GeoClientGRpcAdapter) ListLevenshtein(column, text string) (models.SearchHistoryAddress, error) {
	req := &gp.ListLevenshteinRequest{
		Column: column,
		Text:   text,
	}

	response, err := c.client.ListLevenshtein(context.Background(), req)
	if err != nil {
		return models.SearchHistoryAddress{}, err
	}

	sha := models.SearchHistoryAddress{
		Id:              int(response.Sha.Id),
		SearchRequest:   response.Sha.SearchRequest,
		AddressResponse: response.Sha.AddressResponse,
	}

	return sha, nil
}

func (c *GeoClientGRpcAdapter) Create(sha models.SearchHistoryAddress) error {
	req := &gp.CreateRequest{Sha: &gp.SearchHistoryAddress{
		Id:              int32(sha.Id),
		SearchRequest:   sha.SearchRequest,
		AddressResponse: sha.AddressResponse,
	}}

	_, err := c.client.Create(context.Background(), req)
	return err
}

func (c *GeoClientGRpcAdapter) Search(input []byte) (models.AddressSearchReworked, error) {
	req := &gp.SearchRequest{Place: string(input)}
	response, err := c.client.Search(context.Background(), req)
	if err != nil {
		return nil, err
	}
	var elements []models.AddressElementSearch
	for _, element := range response.Elements {
		elementSearch := models.AddressElementSearch{
			Result: element.GetResult(),
			GeoLat: element.GetGeoLat(),
			GeoLon: element.GetGeoLon(),
		}
		elements = append(elements, elementSearch)
	}
	return elements, nil
}

func (c *GeoClientGRpcAdapter) GeoCode(input []byte) (models.AddressSearchReworked, error) {
	req := &gp.GeoRequest{Coordinates: string(input)}
	response, err := c.client.GeoCode(context.Background(), req)
	if err != nil {
		return nil, err
	}
	var elements []models.AddressElementSearch
	for _, element := range response.Elements {
		elementSearch := models.AddressElementSearch{
			Result: element.GetResult(),
			GeoLat: element.GetGeoLat(),
			GeoLon: element.GetGeoLon(),
		}
		elements = append(elements, elementSearch)
	}
	return elements, nil
}

func Connect(address string) (gp.GeoProviderClient, error) {
	for i := 0; i < 8; i++ {
		_, err := net.Dial("tcp", address)
		if err != nil {
			log.Println("Ошибка при подключении к серверу:", err)
			time.Sleep(3 * time.Second)
			continue
		}

		conn, err := grpc.Dial(address, grpc.WithInsecure())
		client := gp.NewGeoProviderClient(conn)
		log.Println("Клиент подключился по адресу: ", address)

		return client, nil
	}
	return nil, fmt.Errorf("unsuccessful connection")
}
