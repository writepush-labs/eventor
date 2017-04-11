package server

import (
	"encoding/json"
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

type ServerOptions struct {
	Debug     *bool
	Port      *string
	DataPath  *string
	Mirror    *string
}

type server struct {
	es eventstore.Eventstore
	opts *ServerOptions
	logger log.Logger
	storage eventstore.Storage
	introspect eventstore.IntrospectStorage
	echo *echo.Echo
	dispatcher *dispatcher.HttpDispatcher
	isMirroring bool
	websocketConnections *websocketConnectionsMap
}

func intval(number string) int {
	v, err := strconv.Atoi(number)

	if err != nil {
		return 0
	}

	return v
}

// @todo this should be abstracted to some consumeStream method or something with a callback to
func (srv *server) startStreamMirroring(streamName string, startFrom int64) error {
	headers := http.Header{
		"x-start-from": []string{strconv.Itoa(int(startFrom))},
	}
	conn, err := srv.websocketConnections.Dial(streamName + "_mirror", *srv.opts.Mirror + "/streams/" + streamName + "/ws", headers)

	if err != nil {
		return err
	}

	srv.logger.Info("Started mirroring stream from position", log.String("stream", streamName), log.Int64("startFrom", startFrom))
	go srv.consumeStream(conn, true)

	return nil
}

func (srv *server) initWebServer() {
	srv.echo = echo.New()
	srv.echo.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: []string{"*"},
		AllowMethods: []string{echo.GET},
	}))
}

func (srv *server) initStreamRoutes() {
	// retrieve events
	srv.echo.GET("/streams/:stream/:offset/:limit", func(c echo.Context) error {
		events, err := srv.storage.FetchEvents(c.Param("stream"), intval(c.Param("offset")), intval(c.Param("limit")))

		if err != nil {
			return c.String(http.StatusInternalServerError, err.Error()+"\n")
		}

		return c.JSON(http.StatusOK, events)
	})

	// don't accept events in replication mode
	if srv.isMirroring {
		return
	}

	// accept events
	srv.echo.POST("/streams/:stream", func(c echo.Context) error {
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

		persisted := srv.es.AcceptEvent(event)

		if persisted.Error != nil {
			return c.String(http.StatusInternalServerError, "Error occured: "+persisted.Error.Error()+"\n")
		}

		return c.String(http.StatusOK, "Created\n")
	})
}

func (srv *server) initSubscriptionRoutes() {
	// create subscription
	srv.echo.POST("/subscriptions/:name", func(c echo.Context) error {
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

		err = srv.es.AcceptSubscription(subscription)
		if err != nil {
			return c.String(http.StatusBadRequest, err.Error()+"\n")
		}

		return c.String(http.StatusOK, "Created\n")
	})

	// list subscriptions
	srv.echo.GET("/subscriptions", func(c echo.Context) error {
		subscriptions, err := srv.storage.FetchSubscriptions(true)

		if err != nil {
			return c.String(http.StatusBadRequest, err.Error()+"\n")
		}

		return c.JSON(http.StatusOK, subscriptions)
	})

	// delete subscription
	srv.echo.DELETE("/subscriptions/:name", func(c echo.Context) error {
		err := srv.es.RemoveSubscription(c.Param("name"))
		if err != nil {
			return c.String(http.StatusBadRequest, err.Error()+"\n")
		}

		return c.String(http.StatusOK, "Deleted\n")
	})

	// pause subscription
	srv.echo.POST("/subscriptions/:name/pause", func(c echo.Context) error {
		subscriptionName := c.Param("name")

		err := srv.es.PauseSubscription(subscriptionName, "API request")
		if err != nil {
			return c.String(http.StatusBadRequest, err.Error()+"\n")
		}

		return c.String(http.StatusOK, "Paused\n")
	})

	// resume subscription
	srv.echo.POST("/subscriptions/:name/resume", func(c echo.Context) error {
		subscriptionName := c.Param("name")

		err := srv.es.ResumeSubscription(subscriptionName)
		if err != nil {
			return c.String(http.StatusBadRequest, err.Error()+"\n")
		}

		return c.String(http.StatusOK, "Resumed\n")
	})
}

func (srv *server) initIntrospectRoutes() {
	srv.echo.GET("/introspect", func(c echo.Context) error {
		stats := []interface{}{}

		for _, stat := range srv.introspect.GetStats() {
			stats = append(stats, stat)
		}

		return c.JSON(http.StatusOK, stats)
	})

	srv.echo.GET("/introspect/:stream", func(c echo.Context) error {
		stream := c.Param("stream")

		info, _ := srv.introspect.GetStreamInfo(stream)

		return c.JSON(http.StatusOK, info)
	})
}

func (srv *server) initDashboardRoutes() {
	// static dashboard server
	dashboardRoot := "./ui/public"
	if _, err := os.Stat(dashboardRoot); err == nil {
		srv.logger.Info("Serving dev version of dashboard")
		dashboardHandler := http.FileServer(http.Dir(dashboardRoot))
		srv.echo.GET("/dashboard", echo.WrapHandler(http.StripPrefix("/dashboard", dashboardHandler)))
		srv.echo.File("/dashboard/*", "./ui/public/index.html")
		srv.echo.GET("/dashboard/build/*", echo.WrapHandler(http.StripPrefix("/dashboard", dashboardHandler)))
	} else {
		// @todo sort this out
		/*srv.logger.Info("Serving built-in version of dashboard")
		dashboardFs := assetFS()
		pageRoutes := func(c echo.Context) error {
			blob, err := dashboardFs.Asset("index.html")

			if err != nil {
				return c.String(http.StatusBadRequest, err.Error()+"\n")
			}

			return c.HTMLBlob(http.StatusOK, blob)
		}
		srv.echo.GET("/dashboard", pageRoutes)
		srv.echo.GET("/dashboard/*", pageRoutes)
		srv.echo.GET("/dashboard/build/*", func(c echo.Context) error {
			f, err := dashboardFs.Open(c.Param("*"))

			defer f.Close()

			if err != nil {
				return c.String(http.StatusBadRequest, err.Error()+"\n")
			}

			http.ServeContent(c.Response(), c.Request(), c.Param("*"), time.Now(), f)
			return nil
		})*/
	}
}

func (srv *server) initWebsocketRoutes() {
	srv.echo.GET("/subscriptions/:name/ws", func(c echo.Context) error {
		subscriptionName := c.Param("name")

		s, err := srv.storage.FetchSubscription(subscriptionName)

		if err != nil {
			return c.String(http.StatusBadRequest, err.Error()+"\n")
		}

		if len(s.Name) == 0 {
			return c.String(http.StatusNotFound, "Unable to find subscription\n")
		}

		if len(s.Url) != 0 {
			return c.String(http.StatusBadRequest, "Subscription has callback URL and can not be consumed over a Websocket\n")
		}

		_, err = srv.websocketConnections.Upgrade(subscriptionName, c.Response(), c.Request())

		if err != nil {
			return c.String(http.StatusBadRequest, err.Error()+"\n")
		}

		err = srv.es.ResumeSubscription(subscriptionName)

		if err != nil {
			return c.String(http.StatusBadRequest, err.Error()+"\n")
		}

		return nil
	})

	srv.echo.GET("/streams/:name/ws", func(c echo.Context) error {
		s := eventstore.Subscription{
			Name: uuid.NewV4().String(),
			Stream: c.Param("name"),
			IsTransient: true,
			Encoding: "json",
		}

		headers := c.Request().Header

		doCatchup := true

		if _, ok := headers["X-Start-From"]; ok {
			if len(headers["X-Start-From"]) != 0 {
				s.LastReadPosition = int64(intval(headers["X-Start-From"][0]))
			}
		}

		if _, ok := headers["X-No-Catchup"]; ok {
			if len(headers["X-No-Catchup"]) != 0 {
				doCatchup = false
			}
		}

		_, err := srv.websocketConnections.Upgrade(s.Name, c.Response(), c.Request())

		if err != nil {
			return c.String(http.StatusBadRequest, err.Error()+"\n")
		}

		srv.logger.Info("Accepted anonymous subscription to stream", log.String("stream", s.Stream))

		srv.es.LaunchSubscription(s, doCatchup)

		return nil
	})

	if srv.isMirroring {
		return
	}

	srv.echo.GET("/incoming", func(c echo.Context) error {
		ws, err := srv.websocketConnections.Upgrade(uuid.NewV4().String(), c.Response(), c.Request())
		if err != nil {
			return err
		}
		defer ws.Close()

		err = srv.consumeStream(ws, false)

		if err != nil {
			srv.logger.Error("Websocket error", log.String("error", err.Error()))
		}

		return nil
	})
}

func (srv *server) initShutdownCleanup() {
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		<-sigc
		srv.storage.Shutdown()
		srv.introspect.Shutdown()
	}()
}

func (srv *server) consumeStream(conn eventstore.NetworkConnection, isMirroredStream bool) error {
	for {
		event, err := conn.ReadEvent()

		if err != nil {
			return err
		}

		if isMirroredStream {
			event.IsMirrored = true
		} else {
			event.Uuid    = uuid.NewV4().String()
			event.Created = time.Now().String()
		}

		persisted := srv.es.AcceptEvent(event)

		if persisted.Error == nil {
			conn.WriteAckMessage(eventstore.AckMessage{ Success: true, Reason: persisted.Uuid })
		} else {
			conn.WriteAckMessage(eventstore.AckMessage{ Success: false, Reason: persisted.Error.Error() })
		}

		if isMirroredStream && persisted.Stream == eventstore.ACTIVITY_STREAM_NAME && persisted.Type == eventstore.ACTIVITY_EVENT_TYPE_STREAM_CREATED {
			if len(persisted.Meta) != 0 && len(persisted.Meta["streamName"]) != 0 {
				srv.startStreamMirroring(persisted.Meta["streamName"], 0)
			}
		}
	}

	return nil
}

func CreateServer(opts *ServerOptions, logger log.Logger) *server {
	srv := &server{
		opts: opts,
		logger: logger,
	}

	srv.websocketConnections = CreateWebsocketConnectionsMap()
	srv.dispatcher           = dispatcher.CreateHttpDispatcher(logger, srv.websocketConnections)
	srv.storage              = persistence.CreateSqliteStorage(*opts.DataPath, logger)
	srv.introspect           = persistence.CreateIntrospectSqliteStorage()

	es, err := eventstore.Create(srv.storage, srv.dispatcher)

	if err != nil {
		logger.Panic(err.Error(), log.String("when", "Create eventstore"))
	}

	srv.es = es

	srv.es.EnableIntrospect(srv.introspect)

	srv.initShutdownCleanup()

	err = srv.es.LaunchAllSubscriptions()

	if err != nil {
		logger.Panic(err.Error(), log.String("when", "Launch all subscriptions at startup"))
	}

	if len(*opts.Mirror) > 0 {
		logger.Info("Started in mirrored mode")
		srv.isMirroring = true

		mirroredStreams := srv.es.GetMirroredStreams()

		if ! mirroredStreams.KeyExists(eventstore.ACTIVITY_STREAM_NAME) {
			logger.Info("Activity stream is not tracked, adding tracking")
			mirroredStreams.Set(eventstore.ACTIVITY_STREAM_NAME, 0)
		}

		// connect to replicated streams
		for rStream, rPos := range mirroredStreams.GetCopy() {
			err := srv.startStreamMirroring(rStream, rPos)

			if err != nil {
				logger.Error("Unable to mirror stream", log.String("stream", rStream), log.Int64("startFrom", rPos), log.String("error", err.Error()))
			}
		}
	}

	srv.initWebServer()
	srv.initStreamRoutes()
	srv.initSubscriptionRoutes()
	srv.initWebsocketRoutes()

	srv.echo.Logger.Fatal(srv.echo.Start(":" + *opts.Port))

	return srv
}