package dispatcher

import (
	"github.com/writepush-labs/eventor/eventstore"
	log "github.com/writepush-labs/eventor/logging"
	"github.com/gorilla/websocket"
	"net/http"
	"strconv"
	"fmt"
	"time"
	"bytes"
	"errors"
	"sync"
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

type httpDispatcher struct {
	logger log.Logger

	connsMutex sync.Mutex
	conns map[string]*websocket.Conn
}

func (d *httpDispatcher) GetWebsocketConnection(subscriptionName string) (*websocket.Conn, error) {
	d.connsMutex.Lock()
	defer d.connsMutex.Unlock()

	conn, connExists := d.conns[subscriptionName]

	if ! connExists {
		return &websocket.Conn{}, errors.New("Connection does not exist")
	}

	return conn, nil
}

func (d *httpDispatcher) RegisterWebsocketConnection(subscriptionName string, connectionFactory func() (*websocket.Conn, error)) error {
	d.connsMutex.Lock()
	defer d.connsMutex.Unlock()

	_, connExists := d.conns[subscriptionName]

	if connExists {
		return errors.New("Connection for subscription " + subscriptionName + " is already open")
	}

	newConn, err := connectionFactory()

	if err != nil {
		return err
	}

	d.conns[subscriptionName] = newConn

	return nil
}

func (d *httpDispatcher) Dispatch(e eventstore.PersistedEvent, s eventstore.Subscription) error {
	if len(s.Url) == 0 {
		// handle websocket
		conn, err := d.GetWebsocketConnection(s.Name)

		if err != nil {
			d.logger.Error("Unable to open websocket connection", log.String("url", s.Url), log.String("error", err.Error()))
			return nil
		}

		err = conn.WriteJSON(e)

		if err != nil {
			d.logger.Error("Unable to send event to websocket", log.String("url", s.Url), log.String("error", err.Error()))
			return err
		}

		resp := map[string]string{}
		err = conn.ReadJSON(&resp)

		if err != nil {
			d.logger.Error("Received supposed ACK/NACK but can not read json", log.String("url", s.Url), log.String("error", err.Error()))
			return err
		}

		if len(resp["type"]) == 0 {
			d.logger.Error("Received supposed ACK/NACK but type is empty", log.String("url", s.Url), log.String("error", err.Error()))
			return err
		}

		if resp["type"] == "nack" {
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

func (d *httpDispatcher) OnSubscriptionPaused(s eventstore.Subscription) {

}

func (d *httpDispatcher) OnSubscriptionResumed(s eventstore.Subscription) {

}

func CreateHttpDispatcher(logger log.Logger) *httpDispatcher {
	return &httpDispatcher{
		logger: logger,
		conns: make(map[string]*websocket.Conn),
	}
}