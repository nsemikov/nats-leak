package main

import (
	"encoding/json"
	"flag"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	natsURL          = "http://localhost:4222"
	streamName       = "ORDERS"
	streamSubject    = "ORDERS.*"
	durableName      = "durable-name"
	queueGroupName   = "MONITOR.ORDERS"
	consumerName     = "durable-name"
	subscribeSubject = "ORDERS.queue-group"
	sendMessageRange = "1..5"
)

type message struct {
	ID        int
	Timestamp time.Time
}

func main() {
	logger := zap.New(
		zapcore.NewCore(
			zapcore.NewJSONEncoder(zap.NewDevelopmentEncoderConfig()),
			zapcore.AddSync(io.Writer(os.Stdout)),
			zap.NewAtomicLevelAt(zapcore.DebugLevel),
		),
		zap.Development(),
		zap.ErrorOutput(zapcore.AddSync(io.Writer(os.Stderr))),
	)

	flag.StringVar(&natsURL, "url", natsURL, "NATS url")
	flag.StringVar(&streamName, "stream", streamName, "NATS stream name")
	flag.StringVar(&streamSubject, "subject", streamSubject, "NATS subject")
	flag.StringVar(&durableName, "durable", durableName, "NATS durable name")
	flag.StringVar(&queueGroupName, "queue", queueGroupName, "NATS queue name")
	flag.StringVar(&consumerName, "consumer", consumerName, "NATS consumer name")
	flag.StringVar(&subscribeSubject, "subscription", subscribeSubject, "NATS subscription subject name")
	flag.StringVar(&sendMessageRange, "range", sendMessageRange, "Messages IDs range")
	flag.Parse()

	logger.Debug("init", zap.String("sendMessageRange", sendMessageRange))
	messageIDsBoundary := strings.Split(sendMessageRange, "..")
	if len(messageIDsBoundary) != 2 {
		logger.Fatal("failed to parse range: must be x..y")
	}
	messageIDsFrom, err := strconv.ParseInt(messageIDsBoundary[0], 10, 64)
	messageIDsTo, err := strconv.ParseInt(messageIDsBoundary[1], 10, 64)

	nc, err := nats.Connect(natsURL)
	if err != nil {
		logger.Fatal("failed to connect to NATS", zap.Error(err))
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		logger.Fatal("failed to get JetStream", zap.Error(err))
	}
	stream, err := js.StreamInfo(streamName)
	if err != nil {
		logger.Info("failed to get stream", zap.String("stream name", streamName), zap.Error(err))
	}
	if stream == nil {
		stream, err = js.AddStream(&nats.StreamConfig{
			Name:     streamName,
			Subjects: []string{streamSubject},
		})
		if err != nil {
			logger.Fatal("failed to add stream",
				zap.String("stream name", streamName),
				zap.String("stream subject", streamSubject),
				zap.Error(err),
			)
		} else {
			logger.Info("stream added", zap.Any("stream", stream))
		}
	}
	ci, err := js.ConsumerInfo(streamName, consumerName)
	if err != nil {
		logger.Info("failed to get consumer",
			zap.String("stream name", streamName),
			zap.String("consumer name", consumerName),
			zap.Error(err),
		)
	}
	if ci == nil {
		ci, err = js.AddConsumer(streamName, &nats.ConsumerConfig{
			AckPolicy:      nats.AckExplicitPolicy,
			Durable:        durableName,
			AckWait:        5 * time.Second,
			DeliverSubject: queueGroupName,
			DeliverGroup:   queueGroupName,
			DeliverPolicy:  nats.DeliverNewPolicy,
		})
		if err != nil {
			logger.Fatal("failed to add consumer",
				zap.String("stream name", streamName),
				zap.String("consumer name", consumerName),
				zap.Error(err),
			)
		} else {
			logger.Info("consumer added", zap.Any("consumer", ci), zap.Error(err))
		}
	}
	_, err = js.QueueSubscribe(subscribeSubject, queueGroupName, func(m *nats.Msg) {
		var msg message
		if err := json.Unmarshal(m.Data, &msg); err != nil {
			logger.Error("failed to unmarshal message", zap.ByteString("raw", m.Data), zap.Error(err))
			return
		}
		if err := m.Ack(); err != nil {
			logger.Error("failed to send ack", zap.Any("message", msg), zap.Error(err))
			return
		}
		logger.Info("msg ack", zap.Any("message", msg))
	},
		nats.ManualAck(),
		nats.Durable(ci.Config.Durable),
		nats.AckWait(ci.Config.AckWait),
		nats.DeliverSubject(ci.Config.DeliverSubject),
		nats.DeliverNew(),
	)
	if err != nil {
		logger.Fatal("failed to subscribe",
			zap.String("subject name", subscribeSubject),
			zap.String("queue name", queueGroupName),
			zap.Error(err),
		)
	}
	for i := messageIDsFrom; i <= messageIDsTo; i++ {
		msg := &message{
			ID:        int(i),
			Timestamp: time.Now(),
		}
		b, err := json.Marshal(msg)
		if err != nil {
			logger.Fatal("failed to marshal message", zap.Any("message", msg), zap.Error(err))
		}
		_, err = js.Publish(streamSubject, b)
		if err != nil {
			logger.Fatal("failed to publish message",
				zap.String("stream subject", streamSubject),
				zap.Any("message", msg),
				zap.Error(err),
			)
		}
		logger.Info("message published", zap.String("stream subject", streamSubject), zap.Any("message", msg))
	}
}
