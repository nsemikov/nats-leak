package main

import (
	"encoding/json"
	"flag"
	"io"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	natsURL             = "http://localhost:4222"
	streamName          = "ORDERS"
	streamSubject       = "ORDERS.*"
	durableName         = "MONITOR_ORDERS_durable_auto"
	queueGroupName      = "MONITOR.ORDERS.durable.auto"
	consumerName        = durableName
	subjectName         = "ORDERS.queue-group"
	sendMessageRange    = "1..5"
	deliverPolicyString = "last"
	closeTimeout        = 5 * time.Second
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
	flag.StringVar(&durableName, "durable", durableName, "NATS durable name")
	flag.StringVar(&queueGroupName, "queue", queueGroupName, "NATS queue name")
	flag.StringVar(&consumerName, "consumer", consumerName, "NATS consumer name")
	flag.StringVar(&subjectName, "subscription", subjectName, "NATS subscription subject name")
	flag.StringVar(&deliverPolicyString, "deliver-policy", deliverPolicyString, "NATS deliver policy [all,last,new]")
	flag.DurationVar(&closeTimeout, "close-timeout", closeTimeout, "Timeout to drain NATS subscription and close NATS connection without work")
	flag.Parse()

	deliverPolicy, deliverPolicyOpt := deliverPolicyFromString(deliverPolicyString)

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
			DeliverPolicy:  deliverPolicy,
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
	var processed int64
	catched := make(chan struct{})
	defer close(catched)
	sub, err := js.QueueSubscribe(subjectName, queueGroupName, func(m *nats.Msg) {
		var msg message
		if err := json.Unmarshal(m.Data, &msg); err != nil {
			logger.Error("failed to unmarshal message", zap.ByteString("raw", m.Data), zap.Error(err))
			return
		}
		if err := m.Ack(); err != nil {
			logger.Error("failed to send ack", zap.Any("message", msg), zap.Error(err))
			return
		}
		catched <- struct{}{}
		logger.Info("msg ack", zap.Any("message", msg))
	},
		nats.ManualAck(),
		nats.Durable(ci.Config.Durable),
		nats.AckWait(ci.Config.AckWait),
		nats.DeliverSubject(ci.Config.DeliverSubject),
		deliverPolicyOpt,
	)
	if err != nil {
		logger.Fatal("failed to subscribe",
			zap.String("subject name", subjectName),
			zap.String("queue name", queueGroupName),
			zap.Error(err),
		)
	}

	timeout := closeTimeout
	ticker := time.NewTicker(timeout)

LOOP:
	for {
		select {
		case <-ticker.C:
			logger.Info("===== TIMED OUT =====", zap.Int64("processed", processed))
			break LOOP
		case <-catched:
			ticker.Reset(timeout)
		}
	}
	sub.Drain()
	nc.Drain()
}

func deliverPolicyFromString(s string) (nats.DeliverPolicy, nats.SubOpt) {
	switch strings.ToLower(s) {
	case "all":
		return nats.DeliverAllPolicy, nats.DeliverAll()
	case "new":
		return nats.DeliverNewPolicy, nats.DeliverNew()
	case "last":
		fallthrough
	default:
		return nats.DeliverLastPolicy, nats.DeliverLast()
	}
}
