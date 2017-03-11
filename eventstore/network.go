package eventstore

type AckMessage struct {
	Success bool  `json:"success"`
	Reason string `json:"reason"`
}

type NetworkConnection interface {
	ReadEvent() (Event, error)
	WriteEvent(PersistedEvent) error
	ReadAckMessage() (AckMessage, error)
	WriteAckMessage(AckMessage) error
	Close() error
}

type NamedNetworkConnectionsMap interface {
	Get(string) (NetworkConnection, error)
}
