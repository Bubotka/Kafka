package models

type Tabler interface {
	TableName() string
}

type SearchHistoryAddress struct {
	Id              int    `json:"id" db:"id" db_type:"serial PRIMARY KEY"`
	SearchRequest   string `json:"search_request" db:"search_request" db_type:"VARCHAR(255)"`
	AddressResponse string `json:"address_response" db:"address_response" db_type:"VARCHAR(255)"`
}

func (s SearchHistoryAddress) TableName() string {
	return "sha"
}
