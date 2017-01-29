package persistence

import (
	"github.com/writepush-labs/eventor/eventstore"
	log "github.com/writepush-labs/eventor/logging"
	"github.com/asdine/storm"
	"sync"
)

type boltConnections struct {
	sync.Mutex
	conns    map[string]*storm.DB
	logger   log.Logger
	inMemory bool
}

func (c *boltConnections) openDatabase(name string) *storm.DB {
	db, err := storm.Open(name)

	if err != nil {
		panic(err)
	}

	return db
}

func (c *boltConnections) get(name string) *storm.DB {
	c.Lock()

	if c.conns == nil {
		c.conns = make(map[string]*storm.DB)
	}

	conn, connExists := c.conns[name]

	if !connExists {
		conn = c.openDatabase(name)
		c.conns[name] = conn
	}

	c.Unlock()

	return conn
}

type boltEvent struct {
	Position            int `storm:"id,increment"`
	Body                []byte
}

type boltStorage struct {
	logger log.Logger
	connections boltConnections
}

func (stor *boltStorage) PersistEvent(e eventstore.Event) eventstore.PersistedEvent {
	persisted := eventstore.PersistedEvent{}

	bEvent := boltEvent{ Body: e.Body }

	stor.connections.get(e.Stream).Save(&bEvent)

	stor.logger.Info("Written event into stream", log.String("stream", e.Stream), log.Int("newEventPosition", int(bEvent.Position)))

	persisted.Position = int64(bEvent.Position)

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

func (stor *boltStorage) FetchSubscriptions() ([]eventstore.Subscription, error) {
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

func CreateBoltStorage(logger log.Logger) *boltStorage {
	storage := new(boltStorage)
	storage.logger = logger
	storage.connections = boltConnections{}

	return storage
}