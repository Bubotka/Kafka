package cache

import (
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"time"
)

type Redis struct {
	client *redis.Client
}

func NewRedis(cache *redis.Client) *Redis {
	return &Redis{client: cache}
}

func (c *Redis) Set(key string, value interface{}) error {
	marshal, err := json.Marshal(value)
	if err != nil {
		return err
	}

	err = c.client.Set(key, marshal, 5*time.Minute).Err()
	return err
}

func (c *Redis) Get(key string) (string, error) {
	jsonValue, err := c.client.Get(key).Result()
	if err == redis.Nil {
		return "", fmt.Errorf("not found by key %s", key)
	} else if err != nil {
		return "", err
	}

	return jsonValue, nil
}
