package main

import (
	"encoding/json"
	"flag"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"github.com/satori/go.uuid"
	"github.com/writepush-labs/eventor/dispatcher"
	"github.com/writepush-labs/eventor/eventstore"
	log "github.com/writepush-labs/eventor/logging"
	"github.com/writepush-labs/eventor/persistence"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

var VERSION string

type ServerOptions struct {
	Debug    *bool
	Port     *string
	DataPath *string
}

func intval(number string) int {
	v, err := strconv.Atoi(number)

	if err != nil {
		return 0
	}

	return v
}

func main() {
	opts := &ServerOptions{}
	opts.Debug = flag.Bool("prettylog", false, "Output pretty log")
	opts.Port = flag.String("port", "9400", "Port to listen on")
	opts.DataPath = flag.String("data", "./data", "Path to data")

	flag.Parse()

	logger := log.CreateLogger(*opts.Debug)

	logger.Info("Starting Eventor", log.String("version", VERSION))

	storage := persistence.CreateSqliteStorage(*opts.DataPath, logger)
	es, err := eventstore.Create(storage, map[string]eventstore.EventDispatcher{
		"default": dispatcher.CreateHttpDispatcher(logger),
	})

	if err != nil {
		logger.Panic(err.Error(), log.String("when", "Creating eventstore instance"))
	}

	introspect := persistence.CreateIntrospectSqliteStorage()
	es.EnableIntrospect(introspect)

	//es := eventstore.Create(persistence.CreateBoltStorage(logger), dispatcher.CreateHttpDispatcher(logger))
	//es := eventstore.Create(persistence.CreateInMemorySqliteStorage(logger), dispatcher.CreateHttpDispatcher(logger))
	//es := eventstore.Create(persistence.CreateMemoryStorage(logger), dispatcher.CreateHttpDispatcher(logger))

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		<-sigc
		storage.Shutdown()
		introspect.Shutdown()
	}()

	err = es.LaunchAllSubscriptions()

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
			return c.String(http.StatusInternalServerError, "Error occured: "+err.Error()+"\n")
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
			Uuid:    uuid.NewV4().String(),
			Stream:  stream,
			Body:    body,
			Type:    eventType,
			Created: time.Now().String(),
		}

		persisted := es.AcceptEvent(event)

		if persisted.Error != nil {
			return c.String(http.StatusInternalServerError, "Error occured: "+persisted.Error.Error()+"\n")
		}

		return c.String(http.StatusOK, "Created\n")
	})

	e.GET("/streams/:stream/:offset/:limit", func(c echo.Context) error {
		events, err := storage.FetchEvents(c.Param("stream"), intval(c.Param("offset")), intval(c.Param("limit")))

		if err != nil {
			return c.String(http.StatusInternalServerError, err.Error()+"\n")
		}

		return c.JSON(http.StatusOK, events)
	})

	e.POST("/subscriptions/:name", func(c echo.Context) error {
		subscription := eventstore.Subscription{Name: c.Param("name")}

		body, err := ioutil.ReadAll(c.Request().Body)
		if err != nil {
			return c.String(http.StatusBadRequest, err.Error()+"\n")
		}

		err = json.Unmarshal(body, &subscription)
		if err != nil {
			return c.String(http.StatusBadRequest, err.Error()+"\n")
		}

		subscription.IsActive = true

		err = es.AcceptSubscription(subscription)
		if err != nil {
			return c.String(http.StatusBadRequest, err.Error()+"\n")
		}

		return c.String(http.StatusOK, "Created\n")
	})

	e.GET("/subscriptions", func(c echo.Context) error {
		subscriptions, err := storage.FetchSubscriptions(true)

		if err != nil {
			return c.String(http.StatusBadRequest, err.Error()+"\n")
		}

		return c.JSON(http.StatusOK, subscriptions)
	})

	e.DELETE("/subscriptions/:name", func(c echo.Context) error {
		err := es.RemoveSubscription(c.Param("name"))
		if err != nil {
			return c.String(http.StatusBadRequest, err.Error()+"\n")
		}

		return c.String(http.StatusOK, "Deleted\n")
	})

	e.POST("/subscriptions/:name/pause", func(c echo.Context) error {
		subscriptionName := c.Param("name")

		err := es.PauseSubscription(subscriptionName, "API request")
		if err != nil {
			return c.String(http.StatusBadRequest, err.Error()+"\n")
		}

		return c.String(http.StatusOK, "Paused\n")
	})

	e.POST("/subscriptions/:name/resume", func(c echo.Context) error {
		subscriptionName := c.Param("name")

		err := es.ResumeSubscription(subscriptionName)
		if err != nil {
			return c.String(http.StatusBadRequest, err.Error()+"\n")
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

	// static dashboard server
	dashboardRoot := "./ui/public"
	if _, err := os.Stat(dashboardRoot); err == nil {
		logger.Info("Serving dev version of dashboard")
		dashboardHandler := http.FileServer(http.Dir(dashboardRoot))
		e.GET("/dashboard", echo.WrapHandler(http.StripPrefix("/dashboard", dashboardHandler)))
		e.File("/dashboard/*", "./ui/public/index.html")
		e.GET("/dashboard/build/*", echo.WrapHandler(http.StripPrefix("/dashboard", dashboardHandler)))
	} else {
		logger.Info("Serving built-in version of dashboard")
		dashboardFs := assetFS()
		pageRoutes := func(c echo.Context) error {
			blob, err := dashboardFs.Asset("index.html")

			if err != nil {
				return c.String(http.StatusBadRequest, err.Error()+"\n")
			}

			return c.HTMLBlob(http.StatusOK, blob)
		}
		e.GET("/dashboard", pageRoutes)
		e.GET("/dashboard/*", pageRoutes)
		e.GET("/dashboard/build/*", func(c echo.Context) error {
			f, err := dashboardFs.Open(c.Param("*"))

			defer f.Close()

			if err != nil {
				return c.String(http.StatusBadRequest, err.Error()+"\n")
			}

			http.ServeContent(c.Response(), c.Request(), c.Param("*"), time.Now(), f)
			return nil
		})
	}

	e.Logger.Fatal(e.Start(":" + *opts.Port))
}
