package amqp

import (
	"context"
	"encoding/json"
	"github.com/Azure/go-amqp"
	"go.uber.org/zap"
)

const NumRunnerDefault = 3

type ActiveMQ[T any] struct {
	addr       string
	connection *amqp.Conn
	session    *amqp.Session
}

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

func (a *ActiveMQ[T]) Subscribe(destination string, handler func(msg T, correlationId string) bool, numRunner int, logger *zap.Logger) error {
	ctx := context.Background()
	limiter := make(chan bool, numRunner)
	receiver, _ := a.session.NewReceiver(ctx, destination, nil)

	for {
		msg, err := receiver.Receive(ctx, nil)

		if err != nil {
			return err
		}
		limiter <- true
		go a.sub(ctx, receiver, msg, handler, &limiter, logger)
	}
}

func (a *ActiveMQ[T]) sub(ctx context.Context, receiver *amqp.Receiver, msg *amqp.Message, handler func(msg T, correlationId string) bool, limiter *chan bool, logger *zap.Logger) {
	defer subEndHandler(ctx, receiver, msg, limiter, logger)
	msgJson := msg.Value.([]byte)
	var msgStruct T
	correlationId := ""
	if msg.Properties != nil {
		if id, ok := msg.Properties.CorrelationID.(string); ok {
			correlationId = id
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

func subEndHandler(ctx context.Context, receiver *amqp.Receiver, msg *amqp.Message, limiter *chan bool, logger *zap.Logger) {
	if r := recover(); r != nil {
		receiver.RejectMessage(ctx, msg, nil)
		if logger != nil {
			logger.Error("Recovered from panic error", zap.Any("error", r))
		}
	}
	<-*limiter
}
