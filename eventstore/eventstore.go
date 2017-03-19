package eventstore

import (
	"encoding/json"
	"errors"
	"regexp"
	"sync"
	"github.com/writepush-labs/eventor/utils"
	"github.com/satori/go.uuid"
	"time"
)

const ACTIVITY_STREAM_NAME = "_activity"
const ACTIVITY_EVENT_TYPE_STREAM_CREATED = "stream_created"

type Eventstore interface {
	AcceptEvent(Event) PersistedEvent
	AcceptSubscription(Subscription) error
	PauseSubscription(subscriptionName string, reason string) error
	RemoveSubscription(subscriptionName string) error
	ResumeSubscription(subscriptionName string) error
	LaunchSubscription(Subscription, bool)
	LaunchAllSubscriptions() error
	EnableIntrospect(IntrospectStorage)
	GetMirroredStreams() *utils.PersistedCounterMap
}

type Storage interface {
	PersistEvent(Event) PersistedEvent
	PersistSubscription(Subscription) error
	PersistSubscriptionPosition(subscriptionName string, position int64) error
	FetchEvents(streamName string, offset int, limit int) ([]PersistedEvent, error)
	FetchSubscriptions(bool) ([]Subscription, error)
	FetchSubscription(name string) (Subscription, error)
	DeleteSubscription(name string) error

	PersistMirroredStreams(map[string]int64) error
	FetchMirroredStreams() (map[string]int64, error)
	Shutdown()
}

type IntrospectStorage interface {
	RecordEvent(PersistedEvent) error
	GetStats() map[string]StreamStats
	GetStreamInfo(name string) (StreamInfo, error)
	Shutdown()
}

type EventVariant struct {
	Position int `json:"position"`
	Uuid string `json:"uuid"`
	Created string `json:"created"`
	Body string `json:"body"`
}

type StreamInfo struct {
	Name string `json:"name"`
	EventVariants map[string][]EventVariant `json:"event_variants"`
}

type StreamStats struct {
	Name string `json:"name"`
	Total int64 `json:"total"`
	EventTypeStats []map[string]interface{} `json:"event_types"`
}

type EventDispatcher interface {
	Dispatch(PersistedEvent, Subscription) error
	OnSubscriptionPaused(Subscription)
	OnSubscriptionResumed(Subscription)
}

type Event struct {
	Uuid          string
	Stream        string `json:"stream"`
	Type          string `json:"type"`
	Body          []byte `json:"body"`
	PersistedCopy chan PersistedEvent
	Created       string
	IsMirrored    bool
}

type PersistedEvent struct {
	Uuid          string `json:"uuid"`
	Position      int64  `json:"position"`
	Stream        string `json:"stream"`
	Type          string `json:"type"`
	Body          []byte `json:"body"`
	BodyJson      string `json:"body_json"`
	Error         error  `json:"-"`
	IsOverflowing bool   `json:"-"`
	Created       string `json:"created"`
	Meta          map[string]string `json:"-"`
}

type Stream struct {
	Name     string
	incoming chan interface{}
}

type Subscription struct {
	Name             string            `json:"name"`
	Stream           string            `json:"stream"`
	Url              string            `json:"url"`
	LastReadPosition int64             `json:"position"`
	RawHttpHeaders   json.RawMessage   `json:"headers"`
	HttpHeaders      map[string]string `json:"-"`
	Persisted        chan bool         `json:"-"`
	IsNew            bool              `json:"-"`
	IsActive         bool              `json:"active"`
	IsTransient      bool              `json:"-"`
	dispatching      chan interface{}  `json:"-"`
	PauseReason      string            `json:"pause_reason"`
	Created          string            `json:"created"`
	Updated          string            `json:"updated"`
	Encoding         string            `json:"encoding"`
}

type DeleteSubscriptionRequest struct {
	SubscriptionName string
	Deleted          chan bool
}

type DispatchedEvent struct {
	Stream           string
	Position         int64
	SubscriptionName string
}

type StreamCollection struct {
	sync.Mutex
	streams map[string]Stream
}

func (sc *StreamCollection) create(name string, processMessage func(message interface{})) *Stream {
	sc.Lock()
	defer sc.Unlock()

	if sc.streams == nil {
		sc.streams = make(map[string]Stream)
	}

	stream, streamExists := sc.streams[name]

	if !streamExists {
		stream = Stream{Name: name, incoming: make(chan interface{}, 10000)}

		go func() {
			for newMessage := range stream.incoming {
				processMessage(newMessage)
			}
		}()

		sc.streams[name] = stream
	}

	return &stream
}

func (sc *StreamCollection) get(name string) (*Stream, bool) {
	sc.Lock()
	defer sc.Unlock()

	if sc.streams == nil {
		return nil, false
	}

	stream, streamExists := sc.streams[name]

	if !streamExists {
		return nil, false
	}

	return &stream, true
}

type SubscriptionCollection struct {
	sync.Mutex
	collection map[string]Subscription
	groups     map[string]map[string]*Subscription
}

func (subc *SubscriptionCollection) openLiveStream(s *Subscription, bufferSize int, processMessage func(message interface{})) error {
	subc.Lock()
	defer subc.Unlock()

	s.dispatching = make(chan interface{}, bufferSize)

	go func() {
		for newMessage := range s.dispatching {
			processMessage(newMessage)
		}
	}()

	_, groupExists := subc.groups[s.Stream]

	if !groupExists {
		subc.groups[s.Stream] = make(map[string]*Subscription)
	}

	subc.groups[s.Stream][s.Name] = s

	return nil
}

func (subc *SubscriptionCollection) add(s Subscription) error {
	subc.Lock()
	defer subc.Unlock()

	if subc.collection == nil {
		subc.collection = make(map[string]Subscription)
	}

	if subc.groups == nil {
		subc.groups = make(map[string]map[string]*Subscription)
	}

	if len(s.Name) == 0 {
		return errors.New("Subscription must have name!")
	}

	_, subscriptionExists := subc.collection[s.Name]

	if subscriptionExists {
		return errors.New("Subscription already exists")
	}

	subc.collection[s.Name] = s

	return nil
}

func (subc *SubscriptionCollection) get(name string) (*Subscription, bool) {
	subc.Lock()
	defer subc.Unlock()

	subscription, subscriptionExists := subc.collection[name]

	if !subscriptionExists {
		return nil, false
	}

	return &subscription, true
}

func (subc *SubscriptionCollection) getGroup(name string) (map[string]*Subscription, bool) {
	subc.Lock()
	defer subc.Unlock()

	if subc.groups == nil {
		return nil, false
	}

	group, groupExists := subc.groups[name]

	if !groupExists {
		return nil, false
	}

	return group, true
}

func (subc *SubscriptionCollection) remove(name string) bool {
	subc.Lock()
	defer subc.Unlock()

	s, subscriptionExists := subc.collection[name]

	if !subscriptionExists {
		return false
	}

	delete(subc.collection, name)

	_, groupExists := subc.groups[s.Stream]

	if !groupExists {
		return false
	}

	delete(subc.groups[s.Stream], name)

	return true
}

func (subc *SubscriptionCollection) removeFromGroup(s *Subscription) bool {
	subc.Lock()
	defer subc.Unlock()

	_, groupExists := subc.groups[s.Stream]

	if !groupExists {
		return false
	}

	delete(subc.groups[s.Stream], s.Name)

	return true
}

func (subc *SubscriptionCollection) exists(name string) bool {
	_, subscriptionExists := subc.collection[name]
	return subscriptionExists
}

type eventstore struct {
	streams           StreamCollection
	subscriptions     SubscriptionCollection
	storage           Storage
	dispatcher        EventDispatcher
	meta              *Stream
	activity          *Stream
	introspect        *Stream
	introspectEnabled bool
	mirroredStreams   *utils.PersistedCounterMap
}

func (es *eventstore) AcceptEvent(event Event) PersistedEvent {
	// @todo validate stream name!
	if len(event.Stream) == 0 {
		return PersistedEvent{Error: errors.New("Event must have Stream")}
	}

	if len(event.Type) == 0 {
		return PersistedEvent{Error: errors.New("Event must have Type")}
	}

	if len(event.Body) == 0 {
		return PersistedEvent{Error: errors.New("Event must have Body")}
	}

	if event.Stream == ACTIVITY_STREAM_NAME && event.IsMirrored {
		persisted := PersistedEvent{
			Stream: event.Stream,
			Body: event.Body,
			Type: event.Type,
			Created: event.Created,
			Uuid: event.Uuid,
			Position: es.mirroredStreams.Increment(ACTIVITY_STREAM_NAME),
		}

		// only handle stream_created for now
		if event.Type == ACTIVITY_EVENT_TYPE_STREAM_CREATED {
			newStreamData := map[string]string{}

			err := json.Unmarshal(event.Body, &newStreamData)

			if err != nil {
				return PersistedEvent{Error: err}
			}

			if len(newStreamData["name"]) == 0 {
				return PersistedEvent{Error: errors.New("stream_created event doesnt have stream name")}
			}

			es.mirroredStreams.Set(newStreamData["name"], 0)

			persisted.Meta = map[string]string{
				"streamName": newStreamData["name"],
			}
		}

		return persisted
	}

	event.PersistedCopy = make(chan PersistedEvent)

	stream := es.streams.create(event.Stream, func(message interface{}) {
		newEvent, _ := message.(Event)
		newEvent.PersistedCopy <- es.storage.PersistEvent(newEvent)
	})

	stream.incoming <- event
	persisted := <-event.PersistedCopy

	if event.IsMirrored {
		es.mirroredStreams.Increment(event.Stream)
	}

	if (persisted.Position == 1 && ! event.IsMirrored && event.Stream != ACTIVITY_STREAM_NAME) {
		es.dispatchStreamCreated(persisted.Stream)
	}

	es.SendEventToSubscriptions(persisted)

	if es.introspectEnabled {
		es.introspect.incoming <- persisted
	}

	return persisted
}

func (es *eventstore) SendEventToSubscriptions(e PersistedEvent) {
	// each stream has a subscription group
	subscriptionGroup, subscriptionGroupExists := es.subscriptions.getGroup(e.Stream)

	if !subscriptionGroupExists {
		return
	}

	for _, subscription := range subscriptionGroup {
		if len(subscription.dispatching) == (cap(subscription.dispatching) - 1) {
			// okay, we are full here, can't take anymore events on this subscription
			es.subscriptions.removeFromGroup(subscription)
			e.IsOverflowing = true
		}

		select {
		case subscription.dispatching <- e:
		default:
		}
	}
}

func (es *eventstore) DispatchEvent(e PersistedEvent, s Subscription) error {
	err := es.dispatcher.Dispatch(e, s)

	if err != nil {
		if s.IsTransient {
			es.subscriptions.remove(s.Name)
		} else {
			es.PauseSubscription(s.Name, err.Error())
		}
	}

	return err
}

func (es *eventstore) validateNewSubscription(s Subscription) error {
	subscriptionNameIsValid, _ := regexp.MatchString("(?i)^[a-z0-9_]+$", s.Name)

	if !subscriptionNameIsValid {
		return errors.New("Subscription name must match [a-z0-9_] regex. Must be alphanumeric, underscores are allowed.")
	}

	if s.Stream == "" {
		return errors.New("Stream name can not be empty")
	}

	if len(s.RawHttpHeaders) != 0 {
		// validate headers are correct
		headersErrors := json.Unmarshal(s.RawHttpHeaders, &s.HttpHeaders)

		if headersErrors != nil {
			return errors.New("Headers must be a JSON object with key being name of HTTP header")
		}
	}

	existing, err := es.storage.FetchSubscription(s.Name)

	if err != nil {
		return err
	}

	if len(existing.Name) != 0 {
		return errors.New("Subscription with this name already exists")
	}

	return nil
}

func (es *eventstore) AcceptSubscription(s Subscription) error {
	err := es.validateNewSubscription(s)

	if err != nil {
		return err
	}

	s.Persisted = make(chan bool)
	s.IsNew = true
	es.meta.incoming <- s

	<-s.Persisted

	if s.IsActive {
		es.LaunchSubscription(s, true)
	}

	return nil
}

func (es *eventstore) LaunchAllSubscriptions() error {
	subscriptions, err := es.storage.FetchSubscriptions(false)

	if err != nil {
		return err
	}

	for _, s := range subscriptions {
		es.LaunchSubscription(s, true)
	}

	return nil
}

func (es *eventstore) LaunchSubscription(s Subscription, doCatchup bool) {
	es.subscriptions.add(s)

	go func() {
		if (doCatchup) {
			es.catchupSubscription(s)
		} else {
			es.listenForLiveEvents(s)
		}
	}()
}

func (es *eventstore) PauseSubscription(subscriptionName string, reason string) error {
	s, subscriptionExists := es.subscriptions.get(subscriptionName)

	if !subscriptionExists {
		return errors.New("Subscription doesn't exist")
	}

	subscription := *s

	es.subscriptions.remove(subscriptionName)

	subscription.Persisted = make(chan bool)
	subscription.IsNew = false
	subscription.IsActive = false
	subscription.PauseReason = reason

	es.meta.incoming <- subscription
	<-subscription.Persisted

	return nil
}

func (es *eventstore) RemoveSubscription(subscriptionName string) error {
	deleteRequest := DeleteSubscriptionRequest{SubscriptionName: subscriptionName, Deleted: make(chan bool)}

	es.meta.incoming <- deleteRequest
	deleted := <-deleteRequest.Deleted

	if !deleted {
		return errors.New("Unable to delete subscription")
	}

	es.subscriptions.remove(subscriptionName)

	return nil
}

func (es *eventstore) ResumeSubscription(subscriptionName string) error {
	s, err := es.storage.FetchSubscription(subscriptionName)

	if err != nil {
		return err
	}

	if len(s.Name) == 0 {
		return errors.New("Unable to find subscription")
	}

	s.Persisted = make(chan bool)
	s.IsNew = false
	s.IsActive = true

	es.meta.incoming <- s
	<-s.Persisted

	es.LaunchSubscription(s, true)

	return nil
}

func (es *eventstore) catchupSubscription(s Subscription) {
	eventsQueue, err := es.storage.FetchEvents(s.Stream, int(s.LastReadPosition), 100)

	if len(eventsQueue) != 0 {
		for _, e := range eventsQueue {
			if !es.subscriptions.exists(s.Name) {
				return
			}

			err = es.DispatchEvent(e, s)

			if err != nil {
				panic(err)
				return
			}

			s.LastReadPosition = e.Position

			// lets track our succesfully dispatched event
			es.meta.incoming <- DispatchedEvent{SubscriptionName: s.Name, Stream: e.Stream, Position: e.Position}
		}

		// there might be more events to process
		es.catchupSubscription(s)
	} else {
		es.listenForLiveEvents(s)
	}
}

func (es *eventstore) listenForLiveEvents(s Subscription) {
	// this creates buffered stream for "live" subscriptions, once catchup is complete all of the events are dispatched on this stream
	// @todo make buffer size configurable
	es.subscriptions.openLiveStream(&s, 10000, func(message interface{}) {
		if !es.subscriptions.exists(s.Name) {
			return
		}

		newEvent, _ := message.(PersistedEvent)
		err := es.DispatchEvent(newEvent, s)

		if err != nil {
			return
		}

		es.meta.incoming <- DispatchedEvent{SubscriptionName: s.Name, Stream: newEvent.Stream, Position: newEvent.Position}

		if newEvent.IsOverflowing {
			s.LastReadPosition = newEvent.Position
			es.catchupSubscription(s)
		}
	})
}

func (es *eventstore) EnableIntrospect(stor IntrospectStorage) {
	es.introspect = es.streams.create("_introspect", func(message interface{}) {
		switch message.(type) {
		case PersistedEvent:
			event, _ := message.(PersistedEvent)
			stor.RecordEvent(event)

		default:
		}
	})

	es.introspectEnabled = true
}

func (es *eventstore) setupMirroring() error {
	mirroredStreams, err := es.storage.FetchMirroredStreams()

	if err != nil {
		return err
	}

	es.mirroredStreams = utils.CreatePersistedCounterMap(func(streamPositions map[string]int64 ) {
		es.storage.PersistMirroredStreams(streamPositions)
	}, 1)

	es.mirroredStreams.Load(mirroredStreams);

	return nil
}

func (es *eventstore) dispatchStreamCreated(streamName string) {
	es.AcceptEvent(Event{
		Uuid:    uuid.NewV4().String(),
		Stream:  ACTIVITY_STREAM_NAME,
		Body:    []byte("{\"name\":\"" + streamName + "\"}"),
		Type:    ACTIVITY_EVENT_TYPE_STREAM_CREATED,
		Created: time.Now().String(),
	})
}

func (es *eventstore) GetMirroredStreams() *utils.PersistedCounterMap {
	return es.mirroredStreams
}

func Create(storage Storage, dispatcher EventDispatcher) (Eventstore, error) {
	store := new(eventstore)

	store.storage = storage
	store.streams = StreamCollection{}
	store.subscriptions = SubscriptionCollection{}

	// a special meta stream for subscriptions and to track last processed event position
	store.meta = store.streams.create("_meta", func(message interface{}) {
		switch message.(type) {
		case Subscription:
			subscription, _ := message.(Subscription)

			// @todo handle error here!
			store.storage.PersistSubscription(subscription)
			subscription.Persisted <- true

		case DispatchedEvent:
			event, _ := message.(DispatchedEvent)

			// @todo handle error here!
			err := store.storage.PersistSubscriptionPosition(event.SubscriptionName, event.Position)

			if err != nil {
				panic(err)
			}

		case DeleteSubscriptionRequest:
			request, _ := message.(DeleteSubscriptionRequest)

			// @todo handle errors here
			store.storage.DeleteSubscription(request.SubscriptionName)
			request.Deleted <- true

		default:
			panic("Unknown message received in meta stream")

		}
	})

	err := store.setupMirroring()

	if err != nil {
		return store, err
	}

	store.dispatcher = dispatcher

	return store, nil
}
