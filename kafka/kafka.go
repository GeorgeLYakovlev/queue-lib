package kafka

import (
	"context"
	"crypto/tls"
	"log"
	"sync"
	"time"

	"github.com/rstorlabs/queue-lib"
	kafka "github.com/segmentio/kafka-go"
)

type KafkaQueueManagerCreator struct {
}

type KafkaContext struct {
	Timeout time.Duration
	TLS     *tls.Config
	Context context.Context
}

type KafkaQueueManager struct {
	Server  string
	Dialer  *kafka.Dialer
	Conn    *kafka.Conn
	quit    chan bool
	wg      sync.WaitGroup
	Context context.Context
}

type KafkaQueueConfig struct {
	GroupID     string
	Key         string
	AutoConfirm bool
	Context     context.Context
}

type KafkaOutgoingChannel struct {
	Topic   string
	Writer  *kafka.Writer
	Context *KafkaQueueConfig
}

type KafkaTransportData struct {
	message *kafka.Message
	reader  *kafka.Reader
	context context.Context
}

const DefaultTimeout = 30 * time.Second
const DefaultKey = "jsonMessage"

func (kqmc *KafkaQueueManagerCreator) CreateQueueManager(server, user, pass string, context interface{}) (queuelib.QueueManager, error) {
	var kqm KafkaQueueManager
	var kc *KafkaContext
	if context != nil {
		kc = context.(*KafkaContext)
	}

	if kc != nil {
		kqm.Dialer.Timeout = kc.Timeout
		kqm.Dialer.TLS = kc.TLS
		kqm.Context = kc.Context
	} else {
		kqm.Dialer.Timeout = DefaultTimeout
	}
	kqm.Dialer.DualStack = true
	kqm.Server = server
	kqm.quit = make(chan bool, 1)
	err := kqm.CreateConnection()
	return &kqm, err
}

var Kafka KafkaQueueManagerCreator

func (kqm *KafkaQueueManager) CreateTopic(topic string, config interface{}) error {
	if config == nil {
		return queuelib.EWRONGCONFIG
	}
	topicConfig := config.(*kafka.TopicConfig)
	if topicConfig == nil {
		return queuelib.EWRONGCONFIG
	}
	return kqm.Conn.CreateTopics(*topicConfig)
}

func (kqm *KafkaQueueManager) DeleteTopic(topic string) error {
	return kqm.Conn.DeleteTopics(topic)
}

func (kqm *KafkaQueueManager) CreateOutgoingChannel(topic string, channel_type queuelib.OutgoingChannelType, config interface{}) (queuelib.OutgoingChannel, error) {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kqm.Server},
		Topic:    topic,
		Balancer: &kafka.Hash{},
		Dialer:   kqm.Dialer,
	})
	var kqc *KafkaQueueConfig
	if config != nil {
		kqc = config.(*KafkaQueueConfig)
	}
	return &KafkaOutgoingChannel{
		Topic: topic,
		Writer:w,
		Context: kqc,
	}, nil
}

func (kqm * KafkaQueueManager) CreateConnection() error {
	var conn *kafka.Conn
	var err error
	if kqm.Context != nil {
		conn, err = kqm.Dialer.DialContext(kqm.Context, "tcp", kqm.Server)
	} else {
		conn, err = kqm.Dialer.Dial("tcp", kqm.Server)
	}
	if err != nil {
		log.Printf("Failed to dial %s:%s\n", kqm.Server, err)
	}
	kqm.Conn = conn
	return err
}

func (kqm * KafkaQueueManager) CreateReceivingChannel(topic string, ctx interface{}) (chan *queuelib.QueueMessage, error) {
	var kqc *KafkaQueueConfig
	if ctx != nil {
		kqc = ctx.(*KafkaQueueConfig)
	}
	group_id := ""
	if kqc != nil {
		group_id = kqc.GroupID
	}
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kqm.Server},
		GroupID: group_id,
		Topic:   topic,
		Dialer:  kqm.Dialer,
	})
	// conn, err := kqm.CreateConnection(kqc)
	// if err != nil {
	// 	return nil, err
	// }
	kqm.wg.Add(1)
	message_channel := make(chan *queuelib.QueueMessage, 32)
	go func(r *kafka.Reader, message_channel chan *queuelib.QueueMessage) {
		ctx, cancel_func := context.WithCancel(context.Background())
		quit := false
		go func(quit *bool) {
			<-kqm.quit
			// chain quitting
			kqm.quit<-true
			*quit = true
			cancel_func()
		} (&quit)
		for !quit {
			var m kafka.Message
			var err error
			if kqc.AutoConfirm {
				m, err = r.ReadMessage(ctx)
			} else {
				m, err = r.FetchMessage(ctx)
			}
			if err != nil {
				log.Printf("Failed to receive message:%s\n", err)
				break
			}
			qm := queuelib.QueueMessage{
				Topic: m.Topic,
				IntData: int64(m.Partition),
				StringData: string(m.Key),
				TransportData: &KafkaTransportData{
					message: &m,
					reader: r,
				        context: ctx, },
				JsonData: m.Value,
				Timestamp: m.Time,
			}
			message_channel <- &qm
		}
		r.Close()
		kqm.wg.Done()
	} (r, message_channel)
	return message_channel, nil
}

func (kqm * KafkaQueueManager) CommitMessage(message queuelib.QueueMessage) error {
	td := message.TransportData.(*KafkaTransportData)
	if td == nil {
		return queuelib.EWRONGMESSAGE
	}
	return td.reader.CommitMessages(td.context, *td.message)
}

func (kqm * KafkaQueueManager) Close() error {
	kqm.quit <- true
	kqm.wg.Wait()
	return nil
}

func (koc *KafkaOutgoingChannel) Send(queue_message queuelib.QueueMessage) error {
	key := DefaultKey
	if len(queue_message.StringData) > 0 {
		key = queue_message.StringData
	}
	return koc.Writer.WriteMessages(context.Background(), kafka.Message{
		Key: []byte(key),
		Value: queue_message.JsonData,
	})
}

func (koc *KafkaOutgoingChannel) Close() error {
	return koc.Writer.Close()
}
