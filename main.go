package main

import (
	"github.com/writepush-labs/eventor/eventstore"
	"github.com/writepush-labs/eventor/persistence"
	log "github.com/writepush-labs/eventor/logging"
	"net/http"
	"flag"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"io/ioutil"
	"encoding/json"
	"github.com/writepush-labs/eventor/dispatcher"
	"github.com/satori/go.uuid"
	"time"
)

func main() {
	debug := flag.Bool("debug", false, "Debug mode")

	flag.Parse()

	logger := log.CreateLogger(*debug)

	storage := persistence.CreateSqliteStorage(logger)
	es := eventstore.Create(storage, dispatcher.CreateHttpDispatcher(logger))
	introspect := persistence.CreateIntrospectSqliteStorage()
	es.EnableIntrospect(introspect)

	//es := eventstore.Create(persistence.CreateBoltStorage(logger), dispatcher.CreateHttpDispatcher(logger))
	//es := eventstore.Create(persistence.CreateInMemorySqliteStorage(logger), dispatcher.CreateHttpDispatcher(logger))
	//es := eventstore.Create(persistence.CreateMemoryStorage(logger), dispatcher.CreateHttpDispatcher(logger))

	err := es.LaunchAllSubscriptions()

	if err != nil {
		logger.Panic(err.Error(), log.String("when", "Launch all subscriptions at startup"))
	}

	e := echo.New()
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{echo.GET},
	}))

	e.POST("/streams/:stream", func(c echo.Context) error {
		stream := c.Param("stream")

		body, err := ioutil.ReadAll(c.Request().Body)
		if err != nil {
			return c.String(http.StatusInternalServerError, "Error occured: " + err.Error() + "\n")
		}

		var eventType string
		headers := c.Request().Header

		if _, ok := headers["X-Event-Type"]; ok {
			if len(headers["X-Event-Type"]) != 0 {
				eventType = headers["X-Event-Type"][0]
			}
		}

		if len(eventType) == 0 {
			return c.String(http.StatusBadRequest, "Event must have type specified in x-event-type header")
		}

		event := eventstore.Event{
			Uuid: uuid.NewV4().String(),
			Stream: stream,
			Body: body,
			Type: eventType,
			Created: time.Now().String(),
		}

		persisted := es.AcceptEvent(event)

		if persisted.Error != nil {
			return c.String(http.StatusInternalServerError, "Error occured: " + persisted.Error.Error() + "\n")
		}

		return c.String(http.StatusOK, "Created\n")
	})

	e.POST("/subscriptions/:name", func(c echo.Context) error {
		subscription := eventstore.Subscription{ Name: c.Param("name") }

		body, err := ioutil.ReadAll(c.Request().Body)
		if err != nil {
			return c.String(http.StatusBadRequest, err.Error() + "\n")
		}

		err = json.Unmarshal(body, &subscription)
		if err != nil {
			return c.String(http.StatusBadRequest, err.Error() + "\n")
		}

		subscription.IsActive = true

		err = es.AcceptSubscription(subscription)
		if err != nil {
			return c.String(http.StatusBadRequest, err.Error() + "\n")
		}

		return c.String(http.StatusOK, "Created\n")
	})

	e.GET("/subscriptions", func(c echo.Context) error {
		subscriptions, err := storage.FetchSubscriptions(true)

		if err != nil {
			return c.String(http.StatusBadRequest, err.Error() + "\n")
		}

		return c.JSON(http.StatusOK, subscriptions)
	})

	e.DELETE("/subscriptions/:name", func(c echo.Context) error {
		err := es.RemoveSubscription(c.Param("name"))
		if err != nil {
			return c.String(http.StatusBadRequest, err.Error() + "\n")
		}

		return c.String(http.StatusOK, "Deleted\n")
	})

	e.POST("/subscriptions/:name/pause", func(c echo.Context) error {
		subscriptionName := c.Param("name")

		err := es.PauseSubscription(subscriptionName, "API request")
		if err != nil {
			return c.String(http.StatusBadRequest, err.Error() + "\n")
		}

		return c.String(http.StatusOK, "Paused\n")
	})

	e.POST("/subscriptions/:name/resume", func(c echo.Context) error {
		subscriptionName := c.Param("name")

		err := es.ResumeSubscription(subscriptionName)
		if err != nil {
			return c.String(http.StatusBadRequest, err.Error() + "\n")
		}

		return c.String(http.StatusOK, "Resumed\n")
	})

	e.GET("/introspect", func(c echo.Context) error {
		stats := []interface{}{}

		for _, stat := range introspect.GetStats() {
			stats = append(stats, stat)
		}

		return c.JSON(http.StatusOK, stats)
	})

	e.GET("/introspect/:stream", func(c echo.Context) error {
		stream := c.Param("stream")

		info, _ := introspect.GetStreamInfo(stream)

		return c.JSON(http.StatusOK, info)
	})

	e.Logger.Fatal(e.Start(":9400"))
}