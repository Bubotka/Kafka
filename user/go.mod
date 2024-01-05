module github.com/Bubotka/Microservices/user

go 1.19

require (
	github.com/Bubotka/Microservices/geo v0.0.0
	github.com/Masterminds/squirrel v1.5.4
	github.com/golang/protobuf v1.5.3
	github.com/lib/pq v1.10.9
	github.com/stretchr/testify v1.8.2
	google.golang.org/grpc v1.60.1
	google.golang.org/protobuf v1.32.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/jmoiron/sqlx v1.3.5 // indirect
	github.com/lann/builder v0.0.0-20180802200727-47ae307949d0 // indirect
	github.com/lann/ps v0.0.0-20150810152359-62de8c46ede0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.5.1 // indirect
	golang.org/x/net v0.17.0 // indirect
	golang.org/x/sync v0.4.0 // indirect
	golang.org/x/sys v0.13.0 // indirect
	golang.org/x/text v0.13.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20231002182017-d307bd883b97 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace (
	github.com/Bubotka/Microservices/geo v0.0.0 => ../geo
)
