package storage

import (
	"context"
	"github.com/Bubotka/Microservices/geo/domain/models"
)

//go:generate go run github.com/vektra/mockery/v2@v2.36.0 --name=GeoRepository
type GeoRepository interface {
	Create(ctx context.Context, sha models.SearchHistoryAddress) error
	ListLevenshtein(ctx context.Context, columnName, targetText string) (models.SearchHistoryAddress, error)
}
