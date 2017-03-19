package persistence

import (
	"crypto/md5"
	"database/sql"
	"encoding/json"
	_ "github.com/mattn/go-sqlite3"
	"github.com/writepush-labs/eventor/eventstore"
	"github.com/writepush-labs/eventor/utils"
	"os"
	"sort"
	"strings"
	"sync"
	"errors"
	"fmt"
)

type eventVariants struct {
	sync.Mutex
	variants map[string]eventstore.PersistedEvent
}

func (ev *eventVariants) addEvent(e eventstore.PersistedEvent) {
	eventStruct := make(map[string]interface{})

	err := json.Unmarshal(e.Body, &eventStruct)

	if err != nil {
		return
	}

	keys := []string{}

	for k := range eventStruct {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	keys = append(keys, e.Stream, e.Type)

	h := md5.New()

	hash := fmt.Sprintf("%x", h.Sum([]byte(strings.Join(keys, ""))))

	ev.Lock()
	defer ev.Unlock()
	_, variantExists := ev.variants[hash]

	if !variantExists {
		ev.variants[hash] = e
	}
}

func (ev *eventVariants) flushEvents() map[string]eventstore.PersistedEvent {
	ev.Lock()
	defer ev.Unlock()
	cp := ev.variants
	ev.variants = make(map[string]eventstore.PersistedEvent)
	return cp
}

type introspectSqliteStorage struct {
	counters      *utils.PersistedCounterMap
	eventVariants *eventVariants
	conn          *sql.DB
}

func (stor *introspectSqliteStorage) RecordEvent(e eventstore.PersistedEvent) error {
	stor.counters.IncrementMulti(e.Stream + "/" + e.Type, e.Stream)
	stor.eventVariants.addEvent(e)

	return nil
}

func (stor *introspectSqliteStorage) GetStats() map[string]eventstore.StreamStats {
	res := make(map[string]eventstore.StreamStats)

	for key, stat := range stor.counters.GetCopy() {
		stream := strings.SplitN(key, "/", 2)

		if _, ok := res[stream[0]]; ! ok {
			res[stream[0]] = eventstore.StreamStats{ Name: stream[0], Total: 0 }
		}

		if len(stream) == 1 {
			s := res[stream[0]]
			s.Total = stat
			res[stream[0]] = s
		} else {
			s := res[stream[0]]
			s.EventTypeStats = append(s.EventTypeStats, map[string]interface{}{
				"name": stream[1],
				"total": stat,
			})
			res[stream[0]] = s
		}
	}

	return res
}

func (stor *introspectSqliteStorage) LoadStats() error {
	query := "select stream, event_type, stat_value from stream_stats"
	rows, err := stor.conn.Query(query)
	if err != nil {
		return err
	}

	counters := make(map[string]int64)

	for rows.Next() {
		var (
			stream string
			eventType string
			cnt int64
		)
		err = rows.Scan(&stream, &eventType, &cnt)

		if err != nil {
			return errors.New("Failed to scan stream stat from query iterator")
			break
		}

		if len(eventType) == 0 {
			counters[stream] = cnt
		} else {
			counters[stream + "/" + eventType] = cnt
		}
	}

	err = rows.Err()

	rows.Close()

	if err != nil {
		return errors.New("Unable to read stream stats from database")
	}

	return nil
}

func (stor *introspectSqliteStorage) GetStreamInfo(name string) (eventstore.StreamInfo, error) {
	info := eventstore.StreamInfo{
		Name: name,
		EventVariants: make(map[string][]eventstore.EventVariant),
	}

	query := "select event_type, body, position, uuid, created from event_variants where stream = ?"
	rows, err := stor.conn.Query(query, name)
	if err != nil {
		return info, err
	}

	for rows.Next() {
		var (
			eType string
			eBody string
			ePos int
			uuid string
			created string
		)

		err = rows.Scan(&eType, &eBody, &ePos, &uuid, &created)

		if err != nil {
			return info, errors.New("Failed to scan event variant from iterator")
			break
		}

		info.EventVariants[eType] = append(info.EventVariants[eType], eventstore.EventVariant{ Body: eBody, Position: ePos, Uuid: uuid, Created: created })
	}

	err = rows.Err()

	rows.Close()

	if err != nil {
		return info, errors.New("Unable to read event variants from database")
	}

	return info, nil
}

func (stor *introspectSqliteStorage) Shutdown() {
	stor.conn.Close()
}

func (stor *introspectSqliteStorage) openDatabase() {
	dbPath := "./_itrospect.db"
	createSchema := false

	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		createSchema = true
	}

	db, err := sql.Open("sqlite3", dbPath+"?cache=shared")
	if err != nil {
		panic(err)
	}

	if createSchema {
		_, err := db.Exec(`
			create table stream_stats   (stream text, event_type text, stat_value integer, UNIQUE(stream, event_type));
			create table event_variants (hash text, stream text, event_type text, body text, position integer, uuid text, created text, UNIQUE(hash))
		`)
		if err != nil {
			panic(err)
		}
	}

	stor.conn = db
}

func CreateIntrospectSqliteStorage() *introspectSqliteStorage {
	storage := &introspectSqliteStorage{
		eventVariants: &eventVariants{variants: make(map[string]eventstore.PersistedEvent)},
	}

	storage.openDatabase()

	storage.counters = utils.CreatePersistedCounterMap(func(counters map[string]int64) {
		tx, err := storage.conn.Begin()

		if err != nil {
			panic(err)
		}

		stmt, err := tx.Prepare("insert or replace into stream_stats (stream, event_type, stat_value) VALUES (?, ?, ?)")

		if err != nil {
			panic(err)
		}

		for statKey, statValue := range counters {
			var stream, eventType string

			stats := strings.SplitN(statKey, "/", 2)

			stream = stats[0]

			if len(stats) > 1 {
				eventType = stats[1]
			}

			stmt.Exec(stream, eventType, statValue)
		}

		stmt.Close()

		stmt, err = tx.Prepare("insert or replace into event_variants (hash, stream, event_type, body, position, uuid, created) VALUES (?, ?, ?, ?, ?, ?, ?)")

		if err != nil {
			panic(err)
		}

		for hash, variant := range storage.eventVariants.flushEvents() {
			stmt.Exec(hash, variant.Stream, variant.Type, variant.Body, variant.Position, variant.Uuid, variant.Created)
		}

		err = tx.Commit()

		if err != nil {
			panic(err)
		}
	}, 1)

	storage.LoadStats()

	return storage
}
