package persistence

import (
	"github.com/writepush-labs/eventor/eventstore"
	log "github.com/writepush-labs/eventor/logging"
)



type memoryStorage struct {
	logger log.Logger
}

func (stor *memoryStorage) PersistEvent(e eventstore.Event) eventstore.PersistedEvent {
	persisted := eventstore.PersistedEvent{}
	eventPosition := 0

	stor.logger.Info("Written event into stream", log.String("stream", e.Stream), log.Int("newEventPosition", int(eventPosition)))

	persisted.Position = int64(eventPosition)

	return persisted
}

func (stor *memoryStorage) PersistSubscription(s eventstore.Subscription) error {
	stor.logger.Info("Attempting to create subscription", log.String("name", s.Name))

	return nil
}

func (stor *memoryStorage) PersistSubscriptionPosition(subscriptionName string, position int64) error {
	stor.logger.Info("Recorded last event position", log.String("subscription", subscriptionName), log.Int("position", int(position)))

	return nil
}

func (stor *memoryStorage) FetchEvents(streamName string, offset int, limit int) ([]eventstore.PersistedEvent, error) {
	events := []eventstore.PersistedEvent{}

	return events, nil
}

func (stor *memoryStorage) FetchSubscriptions() ([]eventstore.Subscription, error) {
	subscriptions := []eventstore.Subscription{}

	return subscriptions, nil
}

func (stor *memoryStorage) FetchSubscription(name string) (eventstore.Subscription, error) {
	s := eventstore.Subscription{}

	return s, nil
}

func (stor *memoryStorage) DeleteSubscription(name string) error {
	return nil
}

func CreateMemoryStorage(logger log.Logger) *memoryStorage {
	storage := new(memoryStorage)
	storage.logger = logger

	return storage
}