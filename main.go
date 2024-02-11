package main

import (
	"fmt"
	"go.uber.org/zap"
)

const LimitEmail = 15

type Test struct {
	Count int
	Text  string
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	queue := "test/send-msg5"

	activeMqClient, _ := New[Test]("localhost", "5672")

	activeMqClient.Send(queue, Test{
		Count: 100,
		Text:  "ciao come va",
	})

	activeMqClient.SendWithCorrelation(queue, Test{
		Count: 24,
		Text:  "ciao come va",
	}, "3324324243")

	activeMqClient.Subscribe(queue, func(msg Test, correlationId string) bool {
		fmt.Println("sono qui 1")
		fmt.Println(msg)
		return true
	}, NumRunnerDefault,
		logger)

	<-make(chan int)
}
