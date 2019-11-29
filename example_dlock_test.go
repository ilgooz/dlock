package dlock

import (
	"context"

	dlockredis "github.com/ilgooz/dlock/stores/redis"
	"github.com/sirupsen/logrus"
)

func Example() {
	store := dlockredis.New("localhost:6379", 0)
	log := logrus.New()

	// it's OK reuse dl.
	dl := New("lock-key", store, log)

	// do cancelling depending on your logic; for ex. on graceful shutdowns or on a timeout.
	if err := dl.Lock(context.Background()); err != nil {
		log.Fatal(err)
	}
	defer dl.Unlock()

	log.Println("do some work here")
}
