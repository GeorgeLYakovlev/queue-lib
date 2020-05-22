package main

import (
	"encoding/json"
	"fmt"
	"log"
	//	"math/rand"
	"time"

	flags "github.com/jessevdk/go-flags"
	"github.com/rstorlabs/queue-lib"
	"github.com/rstorlabs/queue-lib/kafka"
	kafkago "github.com/segmentio/kafka-go"

)

var args struct {
	Server     string `short:"s" long:"server" description:"Kafka server" default:"localhost:9092"`
	User       string `short:"u" long:"user" description:"Kafka user"`
	Password   string `short:"w" long:"password" description:"Kafka server password"`
	TopicName  string `short:"t" long:"topic" description:"topic name" required:"true"`
	Partitions int    `short:"p" long:"partitions" description:"number of partitions" default:"1"`
	Messages   int    `short:"m" long:"messages" description:"number of messages to generate. If ommitted will receive."`
	Create     bool   `short:"c" long:"create" description:"Create topic, destroy when finished"`
	Destroy    bool   `short:"d" long:"destroy" description:"Destroy topic when finished"`
	UseJson    bool   `short:"j" long:"use_json" description:"Use json for messages"`
}

type StructuredData struct {
	MessageNum  int64 `json:"message_number,omitempty"`
	MessageText string `json:"message_text,omitempty"`
}

var parser = flags.NewParser(&args, flags.Default)

func main() {
	_, err := parser.Parse()
	if err != nil {
		return
	}

	kafka_context := kafka.KafkaContext{
		Timeout: 10 * time.Second,
		TLS: nil,
	}
	queue, err := kafka.Kafka.CreateQueueManager([]string{args.Server}, args.User, args.Password, &kafka_context)
	if err != nil {
		log.Fatalf("Failed to connect to Kafka:%s\n", err)
	}
	if args.Messages > 0 {
		topic_config := kafkago.TopicConfig{
			Topic: args.TopicName,
			NumPartitions: args.Partitions,
			ReplicationFactor: 1,
		}
		if args.Create {
			err = queue.CreateTopic(args.TopicName, &topic_config)
			if err != nil {
				log.Printf("Failed to create partition: %s\n", err)
			}
		}
		if args.Destroy {
			defer queue.DeleteTopic(args.TopicName)
		}
		queue_config := kafka.KafkaQueueConfig{
			GroupID: args.TopicName,
		}
		channel, err := queue.CreateOutgoingChannel(args.TopicName, queuelib.ChannelQueue, &queue_config)
		if err != nil {
			log.Fatalf("Failed to get topic:%s\n", err)
		}
		defer channel.Close()
		for i := 0; i < args.Messages; i++ {
			message := queuelib.QueueMessage{
				Topic: args.TopicName,
				StringData: fmt.Sprintf("Message #%d", i),
			}
			if args.UseJson {
				data := StructuredData{
					MessageNum: int64(i),
					MessageText: message.StringData,
				}
				message.JsonData, _ = json.Marshal(&data)
				message.StringData = fmt.Sprintf("jsonMessage%08d", i)
			}
			err = channel.Send(message)
			if err != nil {
				log.Fatalf("Failed to send message:%s\n", err)
			}
			fmt.Printf("Sent: %+v\n", message)
		}
	} else {
		incoming, err := queue.CreateReceivingChannel(args.TopicName, nil)
		if err != nil {
			log.Fatalf("Failed to create receiving channel:%s\n", err)
		}
		run_loop := true
		for run_loop {
			select {
			case message := <- incoming:
				fmt.Printf("Received: %+v\n", message)
			case <-time.After(60 * time.Second):
				fmt.Printf("Timeout of receiving messages, quitting...\n")
				run_loop = false
			}
		}
	}
}
