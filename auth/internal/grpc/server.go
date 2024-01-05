package grpc

import (
	"github.com/Bubotka/Microservices/auth/internal/services/auth"
	au "github.com/Bubotka/Microservices/auth/pkg/go/auth"
	ugrpc "github.com/Bubotka/Microservices/proxy/pkg/clients/user/grpc"
	"google.golang.org/grpc"
	"log"
	"net"
)

type GrpcServer struct {
	provider ugrpc.UserProviderer
}

func NewGrpcServer(provider ugrpc.UserProviderer) *GrpcServer {
	return &GrpcServer{provider: provider}
}

func (g *GrpcServer) Listen(address string) {
	l, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Ошибка при прослушивании порта: %v", err)
	}

	server := grpc.NewServer()
	au.RegisterAuthServer(server, auth.NewAuthService(g.provider))
	log.Println("Запуск gRPC сервера...")
	if err := server.Serve(l); err != nil {
		log.Fatalf("Ошибка при запуске сервера: %v", err)
	}
}
