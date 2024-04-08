package amqp

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Azure/go-amqp"
	"go.uber.org/zap"
	"sync"
	"time"
)

type backupMessage struct {
	destination   string
	msg           any
	correlationId string
	logger        *zap.Logger
}

const DefaultNumRunner = 3
const secondsBeforeConnectionRetry = 4

var connectionAmqp *amqp.Conn
var ctxConnectionAmqp context.Context
var cancelCtxConnectionAmqp context.CancelFunc
var sessionAmqpForSend *amqp.Session
var mapOfSenders map[string]*amqp.Sender
var mutexForSenders *sync.RWMutex
var mutexForAppendBackupMessages *sync.Mutex
var sliceBackupMessages []backupMessage

func Connect(host string, port string, logger *zap.Logger) error {
	err := connect(host, port, logger)
	mutexForSenders = &sync.RWMutex{}
	mutexForAppendBackupMessages = &sync.Mutex{}
	sliceBackupMessages = make([]backupMessage, 0)
	go monitorAndRetryConnection(host, port, logger)
	if err != nil {
		cancelCtxConnectionAmqp()
	}
	return err
}

func connect(host string, port string, logger *zap.Logger) error {
	addr := "amqp://" + host + ":" + port
	ctx := context.Background()
	opts := amqp.ConnOptions{
		SASLType: amqp.SASLTypeAnonymous(),
	}
	conn, err := amqp.Dial(ctx, addr, &opts)
	if err != nil {
		if logger != nil {
			logger.Error("amqp not connected", zap.Error(err))
		}
		return err
	}

	connectionAmqp = conn
	sessionAmqpForSend, err = conn.NewSession(ctx, nil)
	if err != nil {
		if logger != nil {
			logger.Error("amqp not connected", zap.Error(err))
		}
		return err
	}

	ctxConnectionAmqp, cancelCtxConnectionAmqp = context.WithCancel(context.Background())
	resetOrNewMapOfSenders()

	return nil
}

func monitorAndRetryConnection(host string, port string, logger *zap.Logger) {
	for {
		select {
		case <-ctxConnectionAmqp.Done():
		}
		connectionAmqp.Close()
		for {
			if err := connect(host, port, logger); err == nil {
				for _, message := range sliceBackupMessages {
					SendWithCorrelation(message.destination, message.msg, message.correlationId, message.logger)
				}
				sliceBackupMessages = make([]backupMessage, 0)
				break
			} else {
				time.Sleep(secondsBeforeConnectionRetry * time.Second)
			}
		}
	}
}

func Subscribe[T any](destination string, handler func(msg T, correlationId string) bool, numRunner int, logger *zap.Logger) {
	for {
		ctx := context.Background()
		limiter := make(chan bool, numRunner)
		session, err := connectionAmqp.NewSession(ctx, nil)
		if err != nil {
			cancelCtxConnectionAmqp()
			if logger != nil {
				logger.Error("amqp not subscribed", zap.Error(err))
			}
		} else {
			receiver, _ := session.NewReceiver(ctx, destination, nil)
			for {
				msg, err := receiver.Receive(ctx, nil)

				if err != nil {
					cancelCtxConnectionAmqp()
					if logger != nil {
						logger.Error("amqp receiver not work", zap.Error(err))
					}
					break
				}
				limiter <- true
				go sub(ctx, receiver, msg, handler, &limiter, logger)
			}
		}
		time.Sleep(secondsBeforeConnectionRetry * time.Second)
	}
}

func sub[T any](ctx context.Context, receiver *amqp.Receiver, msg *amqp.Message, handler func(msg T, correlationId string) bool, limiter *chan bool, logger *zap.Logger) {
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

func Send(destination string, msg any, logger *zap.Logger) error {
	return SendWithCorrelation(destination, msg, "", logger)
}

func SendWithCorrelation(destination string, msg any, correlationId string, logger *zap.Logger) error {
	ctx := context.Background()
	sender, err := getSenderForQueue(destination, logger)
	if err != nil {
		appendNotSentMessage(destination, msg, correlationId, logger)
		cancelCtxConnectionAmqp()
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
	err = sender.Send(ctx, &amqpMsg, nil)
	if err != nil {
		resetOrNewMapOfSenders()
		appendNotSentMessage(destination, msg, correlationId, logger)
		cancelCtxConnectionAmqp()
		if logger != nil {
			logger.Error("amqp send not work", zap.Error(err))
		}
	}

	return err
}

func appendNotSentMessage(destination string, msg any, correlationId string, logger *zap.Logger) {
	mutexForAppendBackupMessages.Lock()
	sliceBackupMessages = append(sliceBackupMessages, backupMessage{
		destination:   destination,
		msg:           msg,
		correlationId: correlationId,
		logger:        logger,
	})
	mutexForAppendBackupMessages.Unlock()
}

func resetOrNewMapOfSenders() {
	mapOfSenders = make(map[string]*amqp.Sender)
}

func getSenderForQueue(destination string, logger *zap.Logger) (*amqp.Sender, error) {
	mutexForSenders.RLock()
	sender, found := mapOfSenders[destination]
	mutexForSenders.RUnlock()
	if found {
		return sender, nil
	} else {
		mutexForSenders.Lock()
		sender, err := sessionAmqpForSend.NewSender(context.Background(), destination, nil)
		if err != nil {
			if logger != nil {
				logger.Error("amqp get sender not work", zap.Error(err))
			}
			mutexForSenders.Unlock()
			return nil, errors.New("sender not created")
		}
		mapOfSenders[destination] = sender
		mutexForSenders.Unlock()
		return sender, nil
	}
}
