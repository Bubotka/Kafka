package tests

import (
	"context"
	"github.com/Bubotka/Microservices/geo/domain/models"
	"github.com/Bubotka/Microservices/geo/internal/service"
	"github.com/Bubotka/Microservices/geo/internal/storage/mocks"
	gp "github.com/Bubotka/Microservices/geo/pkg/go/geo"
	"github.com/golang/protobuf/ptypes/empty"
	"reflect"
	"testing"
)

func TestGeoService_Create(t *testing.T) {
	type args struct {
		ctx context.Context
		req *gp.CreateRequest
	}
	tests := []struct {
		name    string
		args    args
		want    *empty.Empty
		wantErr bool
	}{
		{
			name: "base_test",
			args: args{
				ctx: context.Background(),
				req: &gp.CreateRequest{Sha: &gp.SearchHistoryAddress{
					Id:              0,
					SearchRequest:   "Москва",
					AddressResponse: "sdfjsld;gjsdl",
				}},
			},
		},
	}
	geoRepository := mocks.NewGeoRepository(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := service.NewGeoService(geoRepository)
			geoRepository.On("Create", context.Background(), models.SearchHistoryAddress{SearchRequest: "Москва", AddressResponse: "sdfjsld;gjsdl"}).
				Return(nil)
			_, err := g.Create(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("Create() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestGeoService_GeoCode(t *testing.T) {
	type args struct {
		ctx context.Context
		req *gp.GeoRequest
	}
	tests := []struct {
		name    string
		args    args
		want    *gp.GeoResponse
		wantErr bool
	}{
		{
			name: "base_test",
			args: args{
				ctx: context.Background(),
				req: &gp.GeoRequest{Coordinates: "{\"lat\":\"55.746953903751084\",\"lng\":\"37.61718750000001\"}\n"},
			},
			want: &gp.GeoResponse{Elements: []*gp.AddressElementSearch{{
				Result: "г Москва, Софийская наб, д 14 стр 1",
				GeoLat: "55.747",
				GeoLon: "37.617",
			}}},
		},
	}
	geoRepository := mocks.NewGeoRepository(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := service.NewGeoService(geoRepository)
			got, err := g.GeoCode(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("GeoCode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GeoCode() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGeoService_ListLevenshtein(t *testing.T) {
	type args struct {
		ctx context.Context
		req *gp.ListLevenshteinRequest
	}
	tests := []struct {
		name    string
		args    args
		want    *gp.ListLevenshteinResponse
		wantErr bool
	}{
		{
			name: "base_test",
			args: args{
				ctx: context.Background(),
				req: &gp.ListLevenshteinRequest{
					Column: "search_request",
					Text:   "jkljklsdfgjkl",
				},
			},
			want: &gp.ListLevenshteinResponse{Sha: &gp.SearchHistoryAddress{}},
		},
	}
	geoRepository := mocks.NewGeoRepository(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := service.NewGeoService(geoRepository)

			geoRepository.On("ListLevenshtein", context.Background(), "search_request", "jkljklsdfgjkl").
				Return(models.SearchHistoryAddress{}, nil)

			got, err := g.ListLevenshtein(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("ListLevenshtein() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ListLevenshtein() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGeoService_Search(t *testing.T) {
	type args struct {
		ctx context.Context
		req *gp.SearchRequest
	}
	tests := []struct {
		name    string
		args    args
		want    *gp.SearchResponse
		wantErr bool
	}{
		{
			name: "base_test",
			args: args{
				ctx: context.Background(),
				req: &gp.SearchRequest{Place: "{\"query\":\"москва сухонская 11\"}\n"},
			},
			want: &gp.SearchResponse{Elements: []*gp.AddressElementSearch{{
				Result: "г Москва, ул Сухонская, д 11",
				GeoLat: "55.878",
				GeoLon: "37.654",
			}}},
		},
	}
	geoRepository := mocks.NewGeoRepository(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := service.NewGeoService(geoRepository)
			got, err := g.Search(tt.args.ctx, tt.args.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("Search() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Search() got = %v, want %v", got, tt.want)
			}
		})
	}
}
