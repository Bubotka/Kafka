package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Bubotka/Microservices/geo/domain/models"
	"github.com/Bubotka/Microservices/geo/internal/cache"
	"github.com/Bubotka/Microservices/geo/pkg/db/adapter"
)

type GeoStorage struct {
	adapter adapter.SqlAdapterer
	cache   cache.Cache
}

func NewGeoStorage(adapter adapter.SqlAdapterer, cache cache.Cache) *GeoStorage {
	return &GeoStorage{adapter: adapter, cache: cache}
}

func (u *GeoStorage) Create(ctx context.Context, sha models.SearchHistoryAddress) error {
	err := u.adapter.Create(ctx, sha)
	err = u.cache.Set(sha.SearchRequest, sha.AddressResponse)
	if err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Println("Кешик сработал")
	return nil
}

func (u *GeoStorage) ListLevenshtein(ctx context.Context, columnName, targetText string) (models.SearchHistoryAddress, error) {
	get, err := u.cache.Get(targetText)
	if err == nil {
		fmt.Println(get)
		var sha models.SearchHistoryAddress
		var respone string
		json.Unmarshal([]byte(get), &respone)
		sha.AddressResponse = respone
		fmt.Println("Взяли из кеша")
		return sha, nil
	}

	var sha []models.SearchHistoryAddress
	err = u.adapter.ListLevenshtein(ctx, &sha, models.SearchHistoryAddress{}, columnName, targetText)
	if err != nil {
		return models.SearchHistoryAddress{}, err
	}
	if len(sha) == 0 {
		return models.SearchHistoryAddress{}, fmt.Errorf("no such a place")
	}

	return sha[0], nil
}
