package middleware

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
)

type ReverseProxy struct {
	host string
	port string
}

func NewReverseProxy(host, port string) *ReverseProxy {
	return &ReverseProxy{
		host: host,
		port: port,
	}
}

func (rp *ReverseProxy) ReverseProxy(next http.Handler) http.Handler {
	targetUrl, _ := url.Parse(fmt.Sprintf("http://%s:%s", rp.host, rp.port))

	proxy := httputil.NewSingleHostReverseProxy(targetUrl)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/api") || r.URL.Path == "/swagger" {
			next.ServeHTTP(w, r)
		} else if strings.Contains(r.URL.Path, "/static/") {
			next.ServeHTTP(w, r)
		} else {
			proxy.ServeHTTP(w, r)
		}
	})
}
