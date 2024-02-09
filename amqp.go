package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Azure/go-amqp"
)

const NumRunnerDefault = 3

type ActiveMQ[T any] struct {
	addr       string
	connection *amqp.Conn
	session    *amqp.Session
	// logger     Logger
}

// func New[T any](host string, port string, logger Logger) (*ActiveMQ[T], error) {
func New[T any](host string, port string) (*ActiveMQ[T], error) {
	client := &ActiveMQ[T]{addr: "amqp://" + host + ":" + port}
	err := client.connect()
	return client, err
}

func (a *ActiveMQ[T]) connect() error {
	ctx := context.Background()
	opts := amqp.ConnOptions{
		SASLType: amqp.SASLTypeAnonymous(),
	}

	conn, err := amqp.Dial(ctx, a.addr, &opts)
	if err != nil {
		return err
	}

	session, err := conn.NewSession(ctx, nil)
	if err != nil {
		err := conn.Close()
		if err != nil {
			return err
		}
		return err
	}

	a.connection = conn
	a.session = session
	return nil
}

func (a *ActiveMQ[T]) Close(ctx context.Context) error {
	if a.session != nil {
		if err := a.session.Close(ctx); err != nil {
			return err
		}
	}
	if a.connection != nil {
		if err := a.connection.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (a *ActiveMQ[T]) Send(destination string, msg any) error {
	return a.SendWithCorrelation(destination, msg, "")
}

func (a *ActiveMQ[T]) SendWithCorrelation(destination string, msg any, correlationId string) error {
	ctx := context.Background()
	sender, err := a.session.NewSender(ctx, destination, nil)
	defer sender.Close(ctx)
	if err != nil {
		return err
	}

	msgJson, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	amqpMsg := amqp.Message{
		Value: msgJson,
	}

	if correlationId != "" {
		amqpMsg.Properties = &amqp.MessageProperties{
			CorrelationID: correlationId,
		}
	}
	// check send msg format
	return sender.Send(ctx, &amqpMsg, nil)
}

func (a *ActiveMQ[T]) Subscribe(destination string, handler func(msg T, correlationId string) bool, numRunner int) error {
	ctx := context.Background()
	limiter := make(chan bool, numRunner)
	receiver, _ := a.session.NewReceiver(ctx, destination, nil)

	for {
		msg, err := receiver.Receive(ctx, nil)

		if err != nil {
			return err
		}
		limiter <- true
		go a.sub(ctx, receiver, msg, handler, &limiter)
	}
}

func (a *ActiveMQ[T]) sub(ctx context.Context, receiver *amqp.Receiver, msg *amqp.Message, handler func(msg T, correlaionId string) bool, limiter *chan bool) {
	defer subEndHandler(ctx, receiver, msg, limiter)
	msgJson := msg.Value.([]byte)
	var msgStruct T
	var correlationId string
	if msg.Properties != nil {
		if id, ok := msg.Properties.CorrelationID.(string); ok {
			correlationId = id
		} else {
			fmt.Println("CorrelationID is not a string:", msg.Properties.CorrelationID)
		}
	}
	err := json.Unmarshal(msgJson, &msgStruct)

	if err != nil {
		receiver.RejectMessage(ctx, msg, nil)
	} else {
		if handlerErr := handler(msgStruct, correlationId); !handlerErr {
			receiver.RejectMessage(ctx, msg, nil)
		}
		receiver.AcceptMessage(ctx, msg)
	}
}

func subEndHandler(ctx context.Context, receiver *amqp.Receiver, msg *amqp.Message, limiter *chan bool) {
	if r := recover(); r != nil {
		receiver.RejectMessage(ctx, msg, nil)
		// TODO add log
		fmt.Println("Recovered from panic error:", r)
	}
	<-*limiter
}
