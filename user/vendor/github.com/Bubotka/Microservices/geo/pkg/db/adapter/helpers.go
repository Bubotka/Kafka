package adapter

import (
	"reflect"
)

type LimitOffset struct {
	Offset int64
	Limit  int64
}

type Order struct {
	Field string
	Asc   bool
}

type Condition struct {
	Equal       map[string]interface{}
	NotEqual    map[string]interface{}
	Order       []*Order
	LimitOffset *LimitOffset
	ForUpdate   bool
	Upsert      bool
}

type StructInfo struct {
	Fields []string
	Values []interface{}
}

func GetStructInfo(u interface{}) StructInfo {
	val := reflect.ValueOf(u)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}

	var structFields []reflect.StructField

	for i := 0; i < val.NumField(); i++ {
		structFields = append(structFields, val.Type().Field(i))
	}

	var res StructInfo

	for _, field := range structFields {
		valueField := val.FieldByName(field.Name)
		res.Values = append(res.Values, valueField.Interface())
		res.Fields = append(res.Fields, field.Tag.Get("db"))
	}
	return res
}
