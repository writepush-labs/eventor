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
}

func (d *httpDispatcher) Dispatch(e eventstore.PersistedEvent, s eventstore.Subscription) error {
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

func CreateHttpDispatcher(logger log.Logger) *httpDispatcher {
	return &httpDispatcher{logger: logger}
}