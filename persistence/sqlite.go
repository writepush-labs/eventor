package persistence

import (
	"database/sql"
	"encoding/json"
	"errors"
	_ "github.com/mattn/go-sqlite3"
	"github.com/writepush-labs/eventor/eventstore"
	log "github.com/writepush-labs/eventor/logging"
	"os"
	"strconv"
	"sync"
	"time"
)

type connections struct {
	sync.Mutex
	conns    map[string]*sql.DB
	logger   log.Logger
	inMemory bool
}

func (c *connections) openDatabase(name string, createSchemaCallback func(db *sql.DB)) *sql.DB {
	dbPath := "./" + name + ".db"
	createSchema := false

	if c.inMemory {
		dbPath = ":memory:"
		createSchema = true
	} else {
		if _, err := os.Stat(dbPath); os.IsNotExist(err) {
			createSchema = true
		}
	}

	db, err := sql.Open("sqlite3", dbPath+"?cache=shared")
	if err != nil {
		panic(err)
	}

	c.logger.Info("Opened database", log.String("dbPath", dbPath), log.String("dbName", name))

	_, err = db.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		panic(err)
	}

	_, err = db.Exec("PRAGMA synchronous = OFF")
	if err != nil {
		panic(err)
	}

	if createSchema {
		c.logger.Info("Creating DB schema", log.String("dbName", name))
		createSchemaCallback(db)
	}

	return db
}

func (c *connections) get(name string, schemaFactory func(db *sql.DB)) *sql.DB {
	c.Lock()

	if c.conns == nil {
		c.conns = make(map[string]*sql.DB)
	}

	conn, connExists := c.conns[name]

	if !connExists {
		conn = c.openDatabase(name, schemaFactory)
		c.conns[name] = conn
	}

	c.Unlock()

	return conn
}

type sqliteStorage struct {
	connections connections
	logger      log.Logger
}

func (stor *sqliteStorage) getStreamConnection(streamName string) *sql.DB {
	return stor.connections.get(streamName, func(db *sql.DB) {
		_, err := db.Exec("create table events (uuid, body text, type text, created)")
		if err != nil {
			panic(err)
		}
	})
}

func (stor *sqliteStorage) getMetaConnection() *sql.DB {
	return stor.connections.get("_meta", func(db *sql.DB) {
		_, err := db.Exec("create table subscriptions (name, stream, url, httpHeaders, lastReadPosition, isActive, createdAt, updatedAt, pauseReason)")
		if err != nil {
			panic(err)
		}
	})
}

func (stor *sqliteStorage) PersistEvent(e eventstore.Event) eventstore.PersistedEvent {
	persisted := eventstore.PersistedEvent{ Stream: e.Stream, Body: e.Body, Type: e.Type, Created: e.Created, Uuid: e.Uuid }

	tx, err := stor.getStreamConnection(e.Stream).Begin()

	if err != nil {
		stor.logger.Error("Unable to open transaction when writing event to stream", log.String("error", err.Error()), log.String("stream", e.Stream))
		persisted.Error = err
		return persisted
	}

	stmt, err := tx.Prepare("insert into events (uuid, body, type, created) values(?, ?, ?, ?)")
	if err != nil {
		stor.logger.Error("Unable to prepare statement when writing event to stream", log.String("error", err.Error()), log.String("stream", e.Stream))
		persisted.Error = err
		return persisted
	}
	defer stmt.Close()

	res, err := stmt.Exec(e.Uuid, e.Body, e.Type, e.Created)
	if err != nil {
		stor.logger.Error("Unable to execute statement when writing event to stream", log.String("error", err.Error()), log.String("stream", e.Stream))
		persisted.Error = err
		return persisted
	}

	err = tx.Commit()

	if err != nil {
		stor.logger.Error("Unable to commit transaction when writing event to stream", log.String("error", err.Error()), log.String("stream", e.Stream))
		persisted.Error = err
		return persisted
	}

	eventPosition, err := res.LastInsertId()

	if err != nil {
		stor.logger.Error("Unable to obtain new event position", log.String("error", err.Error()), log.String("stream", e.Stream))
		persisted.Error = err
		return persisted
	}

	stor.logger.Info("Written event into stream", log.String("stream", e.Stream), log.Int("newEventPosition", int(eventPosition)))

	persisted.Position = eventPosition

	return persisted
}

func (stor *sqliteStorage) PersistSubscription(s eventstore.Subscription) error {
	tx, err := stor.getMetaConnection().Begin()

	if err != nil {
		return err
	}

	var isActive int

	if s.IsActive {
		isActive = 1
	} else {
		isActive = 0
	}

	if s.IsNew {
		stor.logger.Info("Attempting to create subscription", log.String("name", s.Name))

		var existingStream sql.NullString
		err = tx.QueryRow("select rowid from subscriptions where name = ?", s.Name).Scan(&existingStream)

		if existingStream.Valid {
			tx.Commit()
			return errors.New("Subscription with this name already exists")
		}

		stmt, err := tx.Prepare("insert into subscriptions (name, stream, url, httpHeaders, lastReadPosition, isActive, createdAt) values(?, ?, ?, ?, ?, ?, ?)")
		if err != nil {
			return err
		}

		_, err = stmt.Exec(s.Name, s.Stream, s.Url, string(s.RawHttpHeaders), s.LastReadPosition, isActive, time.Now().String())
		if err != nil {
			return err
		}

		tx.Commit()
		stmt.Close()

		stor.logger.Info("Subscription created and persisted", log.String("name", s.Name))
	} else {
		stor.logger.Info("Updating subscription", log.String("name", s.Name))
		stmt, err := tx.Prepare("update subscriptions set url = ?, httpHeaders = ?, isActive = ?, updatedAt = ?, pauseReason = ? where name = ?")
		if err != nil {
			return err
		}

		_, err = stmt.Exec(s.Url, string(s.RawHttpHeaders), isActive, time.Now().String(), s.PauseReason, s.Name)
		if err != nil {
			return err
		}

		tx.Commit()
		stmt.Close()

		stor.logger.Info("Subscription updated", log.String("name", s.Name))
	}

	return nil
}

func (stor *sqliteStorage) PersistSubscriptionPosition(subscriptionName string, position int64) error {
	tx, err := stor.getMetaConnection().Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare("update subscriptions set lastReadPosition = ? where name = ?")
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(strconv.Itoa(int(position)), subscriptionName)
	if err != nil {
		return err
	}

	tx.Commit()

	stor.logger.Info("Recorded last event position", log.String("subscription", subscriptionName), log.Int("position", int(position)))

	return nil
}

func (stor *sqliteStorage) FetchEvents(streamName string, offset int, limit int) ([]eventstore.PersistedEvent, error) {
	query := "select rowid as position, uuid, body, type, created from events where rowid > " + strconv.Itoa(offset) + " order by rowid limit " + strconv.Itoa(limit)
	rows, err := stor.getStreamConnection(streamName).Query(query)
	if err != nil {
		return nil, err
	}

	events := []eventstore.PersistedEvent{}

	for rows.Next() {
		var event = eventstore.PersistedEvent{ Stream: streamName }
		err = rows.Scan(&event.Position, &event.Uuid, &event.Body, &event.Type, &event.Created)
		if err != nil {
			stor.logger.Error("Failed to scan event from query iterator", log.String("error", err.Error()))
			break
		}

		if event.Body[0] == '[' || event.Body[0] == '{' {
			event.BodyJson = string(event.Body)
		}

		events = append(events, event)
	}

	err = rows.Err()

	rows.Close()

	if err != nil {
		stor.logger.Error("Query iterator returned error", log.String("error", err.Error()))
		return nil, err
	}

	return events, nil
}

func (stor *sqliteStorage) FetchSubscriptions(includeInactive bool) ([]eventstore.Subscription, error) {
	query := "select name, stream, url, httpHeaders, lastReadPosition, isActive, pauseReason, createdAt, updatedAt from subscriptions"

	if ! includeInactive {
		query += " where isActive = 1"
	}

	rows, err := stor.getMetaConnection().Query(query)
	if err != nil {
		return nil, err
	}

	subscriptions := []eventstore.Subscription{}

	for rows.Next() {
		var s = eventstore.Subscription{}

		var isActive int
		var pauseReason, updatedAt sql.NullString

		err = rows.Scan(&s.Name, &s.Stream, &s.Url, &s.RawHttpHeaders, &s.LastReadPosition, &isActive, &pauseReason, &s.Created, &updatedAt)

		if err != nil {
			stor.logger.Error("Unable to scan subscription from query result", log.String("error", err.Error()))
			continue
		}

		if isActive == 1 {
			s.IsActive = true
		}

		if pauseReason.Valid {
			s.PauseReason = pauseReason.String
		}

		if updatedAt.Valid {
			s.Updated = updatedAt.String
		}

		if len(s.RawHttpHeaders) == 0 {
			s.RawHttpHeaders = json.RawMessage("{}")
		} else {
			err = json.Unmarshal(s.RawHttpHeaders, &s.HttpHeaders)
			if err != nil {
				stor.logger.Error("Unable to unmarshal RawHttpHeaders", log.String("error", err.Error()), log.String("subscription", s.Name))
				continue
			}
		}

		subscriptions = append(subscriptions, s)
	}

	err = rows.Err()

	rows.Close()

	if err != nil {
		stor.logger.Error("Query iterator returned error", log.String("error", err.Error()))
		return nil, err
	}

	return subscriptions, nil
}

func (stor *sqliteStorage) FetchSubscription(name string) (eventstore.Subscription, error) {
	query := "select name, stream, url, httpHeaders, lastReadPosition from subscriptions where name = ?"

	s := eventstore.Subscription{}

	err := stor.getMetaConnection().QueryRow(query, name).Scan(&s.Name, &s.Stream, &s.Url, &s.RawHttpHeaders, &s.LastReadPosition)

	if err != nil && err != sql.ErrNoRows {
		return s, err
	}

	if len(s.RawHttpHeaders) != 0 {
		err = json.Unmarshal(s.RawHttpHeaders, &s.HttpHeaders)
		if err != nil {
			stor.logger.Error("Unable to unmarshal RawHttpHeaders", log.String("error", err.Error()), log.String("subscription", s.Name))
			return s, err
		}
	}

	return s, nil
}

func (stor *sqliteStorage) DeleteSubscription(name string) error {
	query := "delete from subscriptions where name = ?"

	_, err := stor.getMetaConnection().Exec(query, name)

	if err != nil {
		return err
	}

	return nil
}

func CreateSqliteStorage(logger log.Logger) *sqliteStorage {
	storage := new(sqliteStorage)
	storage.logger = logger
	storage.connections = connections{}
	storage.connections.logger = logger

	return storage
}

func CreateInMemorySqliteStorage(logger log.Logger) *sqliteStorage {
	storage := new(sqliteStorage)
	storage.logger = logger
	storage.connections = connections{}
	storage.connections.inMemory = true
	storage.connections.logger = logger

	return storage
}
