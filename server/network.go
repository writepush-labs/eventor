package server

import "github.com/writepush-labs/eventor/eventstore"

type AckMessage struct {
	Success bool  `json:"success"`
	Reason string `json:"reason"`
}

type NetworkConnection interface {
	ReadEvent() (eventstore.Event, error)
	WriteEvent(eventstore.PersistedEvent) error
	ReadAckMessage() (AckMessage, error)
	WriteAckMessage(AckMessage) error
	Close() error
}

type NamedNetworkConnectionsMap interface {
	Get(string) (NetworkConnection, error)
}
