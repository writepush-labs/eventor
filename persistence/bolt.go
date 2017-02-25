package persistence

import (
	"github.com/writepush-labs/eventor/eventstore"
	log "github.com/writepush-labs/eventor/logging"
	"github.com/boltdb/bolt"
	"sync"
	"encoding/gob"
	"bytes"
	"encoding/binary"
)

func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

type boltConnections struct {
	sync.Mutex
	conns    map[string]*bolt.DB
	logger   log.Logger
}

func (c *boltConnections) openDatabase(name string) *bolt.DB {
	db, err := bolt.Open("./data/" + name, 0644, &bolt.Options{ InitialMmapSize: 1 << 27 })

	db.NoSync = true

	if err != nil {
		panic(err)
	}

	return db
}

func (c *boltConnections) get(name string) *bolt.DB {
	c.Lock()

	if c.conns == nil {
		c.conns = make(map[string]*bolt.DB)
	}

	conn, connExists := c.conns[name]

	if !connExists {
		conn = c.openDatabase(name)
		conn.Update(func(tx *bolt.Tx) error {
			_, err := tx.CreateBucketIfNotExists([]byte("events"))
			if err != nil {
				panic("create bucket: " + err.Error())
			}
			return nil
		})
		c.conns[name] = conn
	}

	c.Unlock()

	return conn
}

type boltStorage struct {
	logger log.Logger
	connections boltConnections
}

func (stor *boltStorage) PersistEvent(e eventstore.Event) eventstore.PersistedEvent {
	persisted := eventstore.PersistedEvent{ Body: e.Body, Uuid: e.Uuid, Type: e.Type }


	err := stor.connections.get(e.Stream).Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("events"))

		var eventBuf bytes.Buffer

		pos, _ := b.NextSequence()

		persisted.Position = int64(pos)

		enc := gob.NewEncoder(&eventBuf)

		err := enc.Encode(persisted)

		if err != nil {
			return err
		}

		b.Put(itob(pos), eventBuf.Bytes())

		return nil
	})

	if err != nil {
		//stor.logger.Info("Error occured whilst writing event", log.String("error", err.Error()))
	}

	//stor.logger.Info("Written event into stream", log.String("stream", e.Stream), log.Int("newEventPosition", int(persisted.Position)))

	return persisted
}

func (stor *boltStorage) PersistSubscription(s eventstore.Subscription) error {
	stor.logger.Info("Attempting to create subscription", log.String("name", s.Name))

	return nil
}

func (stor *boltStorage) PersistSubscriptionPosition(subscriptionName string, position int64) error {
	stor.logger.Info("Recorded last event position", log.String("subscription", subscriptionName), log.Int("position", int(position)))

	return nil
}

func (stor *boltStorage) FetchEvents(streamName string, offset int, limit int) ([]eventstore.PersistedEvent, error) {
	events := []eventstore.PersistedEvent{}

	return events, nil
}

func (stor *boltStorage) FetchSubscriptions(bool) ([]eventstore.Subscription, error) {
	subscriptions := []eventstore.Subscription{}

	return subscriptions, nil
}

func (stor *boltStorage) FetchSubscription(name string) (eventstore.Subscription, error) {
	s := eventstore.Subscription{}

	return s, nil
}

func (stor *boltStorage) DeleteSubscription(name string) error {
	return nil
}

func (stor *boltStorage) Shutdown() {

}

func CreateBoltStorage(logger log.Logger) *boltStorage {
	storage := new(boltStorage)
	storage.logger = logger
	storage.connections = boltConnections{}

	return storage
}