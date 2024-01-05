package controller

import (
	"encoding/json"
	"fmt"
	"github.com/Bubotka/Microservices/proxy/internal/infrastructure/responder"
	"github.com/Bubotka/Microservices/proxy/internal/models"
	"github.com/Bubotka/Microservices/proxy/internal/modules/geo/service"
	"io/ioutil"

	"net/http"
)

type GeoI interface {
	Search(w http.ResponseWriter, r *http.Request)
	Geo(w http.ResponseWriter, r *http.Request)
}

type GeoController struct {
	service service.GeoI
	responder.Responder
}

func NewGeoController(geo service.GeoI, responder responder.Responder) *GeoController {
	return &GeoController{service: geo, Responder: responder}
}

func (g *GeoController) Search(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		g.Responder.ErrorBadRequest(w, err)
		return
	}

	var searchReq SearchRequest
	json.Unmarshal(body, &searchReq)

	outList := g.service.ListLevenshtein(service.ListlIn{
		Column: "search_request",
		Text:   searchReq.Place,
	})

	if outList.Error == nil {
		w.WriteHeader(http.StatusOK)
		fmt.Println("Через кеш")
		w.Write([]byte(outList.Sha.AddressResponse))
		return
	}

	outSearch := g.service.Search(service.SearchIn{Data: body})

	if err != nil {
		if outSearch.Error.Error() == "failed make ..database for search request" {
			g.Responder.ErrorBadRequest(w, err)
			return
		} else if outSearch.Error.Error() == "failed to do request" {
			g.Responder.ErrorInternal(w, err)
			return
		}
	}

	address, _ := json.Marshal(SearchResponse{outSearch.Addresses})

	g.service.Create(service.CreateIn{SHA: models.SearchHistoryAddress{
		Id:              0,
		SearchRequest:   searchReq.Place,
		AddressResponse: string(address),
	}})

	w.WriteHeader(http.StatusOK)
	g.Responder.OutputJSON(w, SearchResponse{outSearch.Addresses})
}

func (g *GeoController) Geo(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		g.Responder.ErrorBadRequest(w, err)
		return
	}

	out := g.service.Geo(service.GeoIn{Data: body})

	if err != nil {
		if out.Error.Error() == "failed make ..database for service request" {
			g.Responder.ErrorBadRequest(w, err)
			return
		} else if out.Error.Error() == "failed to do request" {
			g.Responder.ErrorInternal(w, err)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	g.Responder.OutputJSON(w, GeocodeResponse{out.Addresses})
}
