package grpc

import (
	"github.com/Bubotka/Microservices/geo/pkg/db/adapter"
	"github.com/Bubotka/Microservices/user/internal/service"
	"github.com/Bubotka/Microservices/user/internal/storage"
	us "github.com/Bubotka/Microservices/user/pkg/go/user"
	"google.golang.org/grpc"
	"log"
	"net"
)

type GrpcServer struct {
	adapter adapter.SqlAdapterer
}

func NewGrpcServer(adapter adapter.SqlAdapterer) *GrpcServer {
	return &GrpcServer{adapter: adapter}
}

func (g *GrpcServer) Listen(address string) {
	l, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Ошибка при прослушивании порта: %v", err)
	}

	server := grpc.NewServer()
	userStorage := storage.NewUserStorage(g.adapter)
	us.RegisterUsererServer(server, service.NewUserService(userStorage))
	log.Println("Запуск gRPC сервера...")
	if err := server.Serve(l); err != nil {
		log.Fatalf("Ошибка при запуске сервера: %v", err)
	}
}
