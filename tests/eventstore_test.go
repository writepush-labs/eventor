package eventor_tests

import (
	"testing"
	"github.com/writepush-labs/eventor/persistence"
	log "github.com/writepush-labs/eventor/logging"
	"github.com/kylelemons/godebug/pretty"
	"reflect"
	"time"
	"os"
	"github.com/writepush-labs/eventor/eventstore"
)

var (
	ES eventstore.Eventstore
	Storage eventstore.Storage
	Dispatcher *fakeDispatcher
)

type fakeDispatcher struct {
	outgoing chan eventstore.PersistedEvent
}

func (fd *fakeDispatcher) Dispatch(e eventstore.PersistedEvent, s eventstore.Subscription) error {
	fd.outgoing <- e
	return nil
}

func createFakeDispatcher() *fakeDispatcher {
	return &fakeDispatcher{ outgoing: make(chan eventstore.PersistedEvent,  10) }
}

func setup() {
	Storage    = persistence.CreateInMemorySqliteStorage(log.CreateLogger(false))
	Dispatcher = createFakeDispatcher()
	ES, _      = eventstore.Create(Storage, map[string]eventstore.EventDispatcher{ "default": Dispatcher })
}

var EventForInactiveStream = eventstore.Event{ Stream: "inactive_test", Type: "fake", Body: []byte("hello world") }
func TestCreateInactiveSubscription(t *testing.T) {
	inactive := eventstore.Subscription{
		Name: "InactiveSubscription",
		Stream: "inactive_test",
		Url: "fakeurl",
	}

	ES.AcceptSubscription(inactive)

	inactivePersisted, _ := Storage.FetchSubscription(inactive.Name)

	if diff := pretty.Compare(inactive, inactivePersisted); diff != "" {
		t.Errorf("Subscription vs persisted diff: (-got +want)\n%s", diff)
	}

	ES.AcceptEvent(EventForInactiveStream)

	time.Sleep(1 * time.Millisecond)

	if len(Dispatcher.outgoing) != 0 {
		t.Error("Dispatcher received events for inactive subscription")
	}
}

func TestResumeSubscription(t *testing.T) {
	ES.ResumeSubscription("InactiveSubscription")

	select {
	case newEvent := <- Dispatcher.outgoing:
		if string(newEvent.Body) != string(EventForInactiveStream.Body) {
			t.Error("Received event on resumed subscription but it does not match what was sent originally")
		}
		return
	case <- time.After(500 * time.Millisecond):
		t.Error("Received nothing on resumed subscription for 500 ms")
	}
}

func TestPauseSubscription(t *testing.T) {
	ES.PauseSubscription("InactiveSubscription", "")

	ES.AcceptEvent(EventForInactiveStream)

	time.Sleep(1 * time.Millisecond)

	if len(Dispatcher.outgoing) != 0 {
		t.Error("Dispatcher received events for inactive subscription")
	}
}

func TestPersistDispatchEventsInRightOrder(t *testing.T) {
	numEvents := 5

	var fakeEvents, receivedEvents [][]byte

	for i := 1; i <= numEvents; i++ {
		eventBody := []byte("Event" + string(i))
		fakeEvents = append(fakeEvents, eventBody)
		ES.AcceptEvent(eventstore.Event{ Stream: "fake", Type: "fake", Body: eventBody })
	}

	ES.AcceptSubscription(eventstore.Subscription{
		Name: "testSubscription",
		Stream: "fake",
		Url: "fakeurl",
		IsActive: true,
	})

	for e := range Dispatcher.outgoing {
		receivedEvents = append(receivedEvents, e.Body)

		if len(receivedEvents) == numEvents {
			break
		}
	}

	if ! reflect.DeepEqual(fakeEvents, receivedEvents) {
		t.Error("Events dont match")
	}
}

func TestMain(m *testing.M) {
	setup()
	retCode := m.Run()
	os.Exit(retCode)
}

