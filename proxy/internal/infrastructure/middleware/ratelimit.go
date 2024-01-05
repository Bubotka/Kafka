package middleware

import (
	"fmt"
	"github.com/dgrijalva/jwt-go"
	"gitlab.com/ptflp/gopubsub/queue"
	"net/http"
	"strings"
	"time"
)

type RateLimit struct {
	limit           int
	currentRequests int
	queuer          queue.MessageQueuer
}

func NewRateLimit(limit int, queuer queue.MessageQueuer) *RateLimit {
	return &RateLimit{limit: limit, queuer: queuer}
}

func (rl *RateLimit) RateLimit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if rl.currentRequests >= rl.limit {
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Println("Всё кончились попытки\n")

			email := getEmailFromJWT(r)

			err := rl.queuer.Publish("limit", []byte(email))
			if err != nil {
				fmt.Println("Чёт не получилось отправить сообщение")
			}
			return
		}
		rl.currentRequests++
		next.ServeHTTP(w, r)
	})
}

func (rl *RateLimit) ResetCurrentRequestsPerTime(timeInSec int) {
	ticker := time.NewTicker(time.Duration(timeInSec) * time.Second)
	go func() {
		for range ticker.C {
			rl.currentRequests = 0
		}
	}()
}

func getEmailFromJWT(r *http.Request) string {
	tokenString := r.Header.Get("Authorization")
	tokenString = strings.TrimPrefix(tokenString, "Bearer ")
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		return []byte("mysecretkey"), nil
	})

	if err != nil {
		fmt.Println("Ошибка при токене", err)
		return ""
	}

	claims, _ := token.Claims.(jwt.MapClaims)
	email := claims["email"].(string)
	return email
}
