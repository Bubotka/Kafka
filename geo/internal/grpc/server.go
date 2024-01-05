package grpc

import (
	"github.com/Bubotka/Microservices/geo/internal/cache"
	"github.com/Bubotka/Microservices/geo/internal/service"
	"github.com/Bubotka/Microservices/geo/internal/storage"
	"github.com/Bubotka/Microservices/geo/pkg/db/adapter"
	gp "github.com/Bubotka/Microservices/geo/pkg/go/geo"
	"google.golang.org/grpc"
	"log"

	"net"
)

type GrpcServer struct {
	adapter adapter.SqlAdapterer
	cache   cache.Cache
}

func NewGrpcServer(adapter adapter.SqlAdapterer, cache cache.Cache) *GrpcServer {
	return &GrpcServer{adapter: adapter, cache: cache}
}

func (g *GrpcServer) Listen(address string) {
	l, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Ошибка при прослушивании порта: %v", err)
	}

	server := grpc.NewServer()
	geoStorage := storage.NewGeoStorage(g.adapter, g.cache)
	gp.RegisterGeoProviderServer(server, service.NewGeoService(geoStorage))

	log.Println("Запуск gRPC сервера...")
	if err := server.Serve(l); err != nil {
		log.Fatalf("Ошибка при запуске сервера: %v", err)
	}
}
