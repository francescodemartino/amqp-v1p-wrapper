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

type SubscriptionOptions struct {
	ReceiverOptions        *amqp.ReceiverOptions
	MaxTimeWithoutMessages *time.Duration
}

type backupMessage struct {
	destination   string
	msg           any
	msgProperties *amqp.MessageProperties
	logger        *zap.Logger
}

type receiveResponse struct {
	msg *amqp.Message
	err error
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
					SendWithOptions(message.destination, message.msg, message.msgProperties, message.logger)
				}
				sliceBackupMessages = make([]backupMessage, 0)
				break
			} else {
				time.Sleep(secondsBeforeConnectionRetry * time.Second)
			}
		}
	}
}

func receiveMessage(receiver *amqp.Receiver) (<-chan receiveResponse, *chan bool) {
	receiveResponseChan := make(chan receiveResponse)
	doneChan := make(chan bool)
	ctx := context.Background()
	go func() {
		for {
			msg, err := receiver.Receive(ctx, nil)
			var linkError *amqp.LinkError
			if errors.As(err, &linkError) {
				return
			}
			receiveResponseChan <- receiveResponse{msg: msg, err: err}
			outMsg := <-doneChan
			if !outMsg {
				return
			}
		}
	}()
	return receiveResponseChan, &doneChan
}

func SubscribeWithOptions[T any](destination string, handler func(msg T, correlationId string) bool, subscriptionOptions *SubscriptionOptions, numRunner int, logger *zap.Logger) {
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
			var receiverOptions *amqp.ReceiverOptions = nil
			if subscriptionOptions != nil {
				receiverOptions = subscriptionOptions.ReceiverOptions
			}
			receiver, _ := session.NewReceiver(ctx, destination, receiverOptions)
			receiveResponseChan, doneChan := receiveMessage(receiver)

			for {
				var rp receiveResponse

				if subscriptionOptions != nil && subscriptionOptions.MaxTimeWithoutMessages != nil {
					isTimeoutReached := false
					select {
					case rp = <-receiveResponseChan:
					case <-time.After(*subscriptionOptions.MaxTimeWithoutMessages):
						isTimeoutReached = true
					}
					if isTimeoutReached {
						receiver.Close(ctx)
						session.Close(ctx)
						return
					}
				} else {
					rp = <-receiveResponseChan
				}

				if rp.err != nil {
					cancelCtxConnectionAmqp()
					if logger != nil {
						logger.Error("amqp receiver not work", zap.Error(err))
					}
					*doneChan <- false
					break
				}
				limiter <- true
				go sub(ctx, receiver, rp.msg, handler, &limiter, logger)
				*doneChan <- true
			}
		}
		time.Sleep(secondsBeforeConnectionRetry * time.Second)
	}
}

func Subscribe[T any](destination string, handler func(msg T, correlationId string) bool, numRunner int, logger *zap.Logger) {
	SubscribeWithOptions(destination, handler, nil, numRunner, logger)
}

func sub[T any](ctx context.Context, receiver *amqp.Receiver, msg *amqp.Message, handler func(msg T, correlationId string) bool, limiter *chan bool, logger *zap.Logger) {
	defer subEndHandler(ctx, receiver, msg, limiter, logger)
	var msgJson []byte
	var okCasting bool

	if msg.Value == nil {
		msgJson = msg.GetData()
	} else {
		msgJson, okCasting = msg.Value.([]byte)
		if !okCasting {
			msgJson = []byte(msg.Value.(string))
		}
	}

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
		if done := handler(msgStruct, correlationId); done {
			receiver.AcceptMessage(ctx, msg)
		} else {
			receiver.RejectMessage(ctx, msg, nil)
		}
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

func SendWithOptions(destination string, msg any, msgProperties *amqp.MessageProperties, logger *zap.Logger) error {
	ctx := context.Background()
	sender, err := getSenderForQueue(destination, logger)
	if err != nil {
		appendNotSentMessage(destination, msg, msgProperties, logger)
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

	amqpMsg.Header = &amqp.MessageHeader{
		Durable: true,
	}

	if msgProperties != nil {
		amqpMsg.Properties = msgProperties
	}

	// check send msg format
	err = sender.Send(ctx, &amqpMsg, nil)
	if err != nil {
		resetOrNewMapOfSenders()
		appendNotSentMessage(destination, msg, msgProperties, logger)
		cancelCtxConnectionAmqp()
		if logger != nil {
			logger.Error("amqp send not work", zap.Error(err))
		}
	}

	return err
}

func Send(destination string, msg any, logger *zap.Logger) error {
	return SendWithOptions(destination, msg, nil, logger)
}

func SendWithCorrelation(destination string, msg any, correlationId string, logger *zap.Logger) error {
	properties := amqp.MessageProperties{
		CorrelationID: correlationId,
	}
	return SendWithOptions(destination, msg, &properties, logger)
}

func appendNotSentMessage(destination string, msg any, msgProperties *amqp.MessageProperties, logger *zap.Logger) {
	mutexForAppendBackupMessages.Lock()
	sliceBackupMessages = append(sliceBackupMessages, backupMessage{
		destination:   destination,
		msg:           msg,
		msgProperties: msgProperties,
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
