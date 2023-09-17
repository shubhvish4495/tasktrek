package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"tasktrek"
)

func main() {
	ctx := context.Background()
	serv, closeFn, err := tasktrek.NewService(ctx, "amqp://admin:admin@localhost:5672", "testQueue")
	if err != nil {
		panic(err)
	}

	defer closeFn(serv)
	serv.RegisterTaskFunc(func(ctx context.Context, tm tasktrek.TaskMsg, rC chan interface{}) error {
		var err error
		if rand.Int()%2 == 0 {
			err = errors.New("random error")
		}

		rC <- string(tm.GetData())
		return err
	})

	serv.StartProcessing(ctx)

	for data := range serv.AwaitProcessing(ctx) {
		fmt.Println("inside msg receiver func")
		fmt.Println(data)
	}
}
