package main

import (
	"github.com/Bubotka/Microservices/auth/internal/grpc"
	ugrpc "github.com/Bubotka/Microservices/proxy/pkg/clients/user/grpc"
	"github.com/Bubotka/Microservices/proxy/pkg/clients/user/grpc/client_adapter"
	"log"
)

func main() {
	connect, err := client_adapter.Connect("user:8083")
	if err != nil {
		log.Fatal(err)
	}

	userClientGRpcAdapter := client_adapter.NewUserClientGRpcAdapter(connect)

	userProvider := ugrpc.NewUserProvider(userClientGRpcAdapter)

	grpcServer := grpc.NewGrpcServer(userProvider)
	grpcServer.Listen(":8082")
}
