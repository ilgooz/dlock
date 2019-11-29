package dlockredis

import (
	"time"

	redis "github.com/go-redis/redis/v7"
)

// RedisStore implements dlock.Store.
type RedisStore struct {
	c *redis.Client
}

// New creates a new Redis store with addr and db.
// use 0 for db to select default one.
func New(addr string, db int) *RedisStore {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
		DB:   db,
	})
	return &RedisStore{
		c: client,
	}
}

// Set sets key with value and ttl.
// if replace is not allowed and there is a value set for the key already,
// isSet will return with false.
func (r *RedisStore) Set(key string, value interface{}, replace bool, ttl time.Duration) (isSet bool, err error) {
	if !replace {
		return r.c.SetNX(key, value, ttl).Result()
	}
	_, err = r.c.Set(key, value, ttl).Result()
	return err == nil, err
}

// Delete deletes key.
func (r *RedisStore) Delete(key string) (err error) {
	_, err = r.c.Del(key).Result()
	return err
}
