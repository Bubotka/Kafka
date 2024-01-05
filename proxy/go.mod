module github.com/Bubotka/Microservices/proxy

go 1.19

require (
	github.com/Bubotka/Microservices/auth v0.0.0
	github.com/Bubotka/Microservices/geo v0.0.0
	github.com/Bubotka/Microservices/user v0.0.0
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/go-chi/chi v1.5.5
	github.com/go-chi/chi/v5 v5.0.11
	github.com/go-chi/jwtauth v1.2.0
	github.com/golang/protobuf v1.5.3
	github.com/lib/pq v1.10.9
	github.com/ptflp/godecoder v0.0.1
	github.com/streadway/amqp v1.1.0
	github.com/stretchr/testify v1.8.4
	gitlab.com/ptflp/gopubsub v1.1.2
	go.uber.org/zap v1.26.0
	google.golang.org/grpc v1.60.1
)

require (
	github.com/confluentinc/confluent-kafka-go v1.9.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/goccy/go-json v0.3.5 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/lestrrat-go/backoff/v2 v2.0.7 // indirect
	github.com/lestrrat-go/httpcc v1.0.0 // indirect
	github.com/lestrrat-go/iter v1.0.0 // indirect
	github.com/lestrrat-go/jwx v1.1.0 // indirect
	github.com/lestrrat-go/option v1.0.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.5.1 // indirect
	go.uber.org/goleak v1.2.1 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	golang.org/x/crypto v0.14.0 // indirect
	golang.org/x/net v0.17.0 // indirect
	golang.org/x/sys v0.13.0 // indirect
	golang.org/x/text v0.13.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20231002182017-d307bd883b97 // indirect
	google.golang.org/protobuf v1.32.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace (
	github.com/Bubotka/Microservices/auth v0.0.0 => ../auth
	github.com/Bubotka/Microservices/geo v0.0.0 => ../geo
	github.com/Bubotka/Microservices/user v0.0.0 => ../user
)
