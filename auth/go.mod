module github.com/Bubotka/Microservices/auth

go 1.19

require (
	github.com/Bubotka/Microservices/proxy v0.0.0
	github.com/Bubotka/Microservices/user v0.0.0
	github.com/go-chi/jwtauth v1.2.0
	github.com/golang/protobuf v1.5.3
	golang.org/x/crypto v0.14.0
	google.golang.org/grpc v1.60.1
	google.golang.org/protobuf v1.32.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/goccy/go-json v0.3.5 // indirect
	github.com/lestrrat-go/backoff/v2 v2.0.7 // indirect
	github.com/lestrrat-go/httpcc v1.0.0 // indirect
	github.com/lestrrat-go/iter v1.0.0 // indirect
	github.com/lestrrat-go/jwx v1.1.0 // indirect
	github.com/lestrrat-go/option v1.0.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.5.1 // indirect
	github.com/stretchr/testify v1.8.4 // indirect
	golang.org/x/net v0.17.0 // indirect
	golang.org/x/sys v0.13.0 // indirect
	golang.org/x/text v0.13.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20231002182017-d307bd883b97 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace (
	github.com/Bubotka/Microservices/geo v0.0.0 => ../geo
	github.com/Bubotka/Microservices/proxy v0.0.0 => ../proxy
	github.com/Bubotka/Microservices/user v0.0.0 => ../user
)
