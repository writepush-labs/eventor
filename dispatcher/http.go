package dispatcher

import (
	"github.com/writepush-labs/eventor/eventstore"
	log "github.com/writepush-labs/eventor/logging"
	"net/http"
	"strconv"
	"fmt"
	"time"
	"bytes"
	"errors"
	"github.com/writepush-labs/eventor/server"
)

func retry(callback func() error, numRetries int, waitSeconds int) error {
	numAttempts := 0

	backoffRate := 1

	var err error

	for numAttempts < numRetries {
		if numAttempts > 0 {
			time.Sleep(time.Duration(waitSeconds * backoffRate) * time.Second)
		}

		err = callback()

		if err == nil {
			return nil
		}

		numAttempts += 1
		backoffRate = backoffRate * 2
	}

	return err
}

type HttpDispatcher struct {
	logger log.Logger
	websocketConnections server.NamedNetworkConnectionsMap
}

func (d *HttpDispatcher) Dispatch(e eventstore.PersistedEvent, s eventstore.Subscription) error {
	if len(s.Url) == 0 {
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

	return retry(func() error {
		d.logger.Info("Sending out request to endpoint", log.String("url", s.Url), log.Int("eventPosition", int(e.Position)), log.String("subscription", s.Name))

		req, err := http.NewRequest("POST", s.Url, bytes.NewBuffer([]byte(e.Body)))
		req.Header.Set("content-type", "application/json")
		req.Header.Set("x-event-position", strconv.Itoa(int(e.Position)))
		req.Header.Set("x-event-uuid", e.Uuid)
		req.Header.Set("x-event-created", e.Created)
		req.Header.Set("user-agent", "Eventor")

		for headerName, headerValue := range s.HttpHeaders {
			req.Header.Set(headerName, headerValue)
		}

		client := &http.Client{}
		resp, err := client.Do(req)

		if err != nil {
			return err
		}

		resp.Body.Close()

		if resp.StatusCode > 300 {
			d.logger.Warn("HTTP call failed", log.String("url", s.Url), log.Int("statusCode", resp.StatusCode), log.Int("eventPosition", int(e.Position)), log.String("subscription", s.Name))
			return errors.New(fmt.Sprintf("HTTP call to %s failed with error code %s", s.Url, resp.StatusCode))
		}

		return nil
	}, 5, 4)
}

func (d *HttpDispatcher) OnSubscriptionPaused(s eventstore.Subscription) {

}

func (d *HttpDispatcher) OnSubscriptionResumed(s eventstore.Subscription) {

}

func CreateHttpDispatcher(logger log.Logger, websocketConnections server.NamedNetworkConnectionsMap) *HttpDispatcher {
	return &HttpDispatcher{
		logger: logger,
		websocketConnections: websocketConnections,
	}
}