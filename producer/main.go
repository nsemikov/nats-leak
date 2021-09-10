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
	subjectName      = "ORDERS.queue-group"
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
	).With(zap.Int("pid", os.Getpid()))

	flag.StringVar(&natsURL, "url", natsURL, "NATS url")
	flag.StringVar(&streamName, "stream", streamName, "NATS stream name")
	flag.StringVar(&streamSubject, "subject", streamSubject, "NATS subject")
	flag.StringVar(&subjectName, "subscription", subjectName, "NATS subscription subject name")
	flag.StringVar(&sendMessageRange, "range", sendMessageRange, "Messages IDs range")
	flag.Parse()

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
	for i := messageIDsFrom; i <= messageIDsTo; i++ {
		msg := &message{
			ID:        int(i),
			Timestamp: time.Now(),
		}
		b, err := json.Marshal(msg)
		if err != nil {
			logger.Fatal("failed to marshal message", zap.Any("message", msg), zap.Error(err))
		}
		_, err = js.Publish(subjectName, b)
		if err != nil {
			logger.Fatal("failed to publish message",
				zap.String("subject name", subjectName),
				zap.Any("message", msg),
				zap.Error(err),
			)
		}
		logger.Info("message published", zap.String("subject name", subjectName), zap.Any("message", msg))
	}
	nc.Drain()
}
