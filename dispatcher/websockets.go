package dispatcher


import (
	"github.com/writepush-labs/eventor/eventstore"
	log "github.com/writepush-labs/eventor/logging"
	"errors"
)

type WebsocketsDispatcher struct {
	logger log.Logger
	websocketConnections eventstore.NamedNetworkConnectionsMap
}

func (d *WebsocketsDispatcher) Dispatch(e eventstore.PersistedEvent, s eventstore.Subscription) error {
	conn, err := d.websocketConnections.Get(s.Name)

	if err != nil {
		d.logger.Error("Unable to get websocket connection", log.String("error", err.Error()))
		return err
	}

	err = conn.WriteEvent(e)

	if err != nil {
		d.logger.Error("Unable to send event to websocket", log.String("error", err.Error()))
		return err
	}

	ack, err := conn.ReadAckMessage()

	if err != nil {
		d.logger.Error("Received supposed ACK/NACK but can not read json", log.String("error", err.Error()))
		return err
	}

	if ! ack.Success {
		return errors.New("Received NACK from endpoint")
	}

	return nil
}

func (d *WebsocketsDispatcher) OnSubscriptionPaused(s eventstore.Subscription) {

}

func (d *WebsocketsDispatcher) OnSubscriptionResumed(s eventstore.Subscription) {

}

func CreateWebsocketsDispatcher(logger log.Logger, websocketConnections eventstore.NamedNetworkConnectionsMap) *WebsocketsDispatcher {
	return &WebsocketsDispatcher{
		logger: logger,
		websocketConnections: websocketConnections,
	}
}