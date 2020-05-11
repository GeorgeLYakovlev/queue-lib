package queuelib

import (
	//	"encoding/json"
	"time"
	"errors"
)

var EWRONGCONFIG = errors.New("Wrong config was supplied!")
var EWRONGMESSAGE = errors.New("Wrong message to commit!")

type QueueManagerConnector interface {
	/**
         * Connects to server with provided credentials and creates a queue manager.
         */
	CreateQueueManager(server []string, user, pass string, context interface{}) (QueueManager, error)
}

type OutgoingChannelType int

const (
	ChannelQueue OutgoingChannelType = iota
	ChannelFan
)

type QueueMessage struct {
	Topic         string
	IntData       int64
	StringData    string
	TransportData interface{}
	JsonData      []byte // json.RawMessage
	Timestamp     time.Time
}

type QueueManager interface {
	SetClientId(id string)
	GetClientId() string
	CreateTopic(topic string, config interface{}) error
	DeleteTopic(topic string) error
	CreateOutgoingChannel(topic string, channel_type OutgoingChannelType, config interface{}) (OutgoingChannel, error)
	CreateReceivingChannel(topic string, config interface{}) (chan *QueueMessage, error)
	// Config can be set to be either auto-confirm or manual confirm
	// This function commits it manually
	CommitMessage(message QueueMessage) error
	Close() error
}

type OutgoingChannel interface {
	Send(queue_message QueueMessage) error
	Close() error
}


