package server

import (
	"sync"
	"github.com/gorilla/websocket"
	"errors"
	"net/http"
	"github.com/writepush-labs/eventor/eventstore"
)

type websocketConnection struct {
	conn *websocket.Conn
}

func (ws *websocketConnection) ReadEvent() (eventstore.Event, error) {
	event := eventstore.Event{}
	err := ws.conn.ReadJSON(&event)

	if err != nil {
		return event, err
	}

	return event, nil
}

func (ws *websocketConnection) WriteEvent(event eventstore.PersistedEvent) error {
	err := ws.conn.WriteJSON(event)

	if err != nil {
		return err
	}

	return nil
}

func (ws *websocketConnection) ReadAckMessage() (eventstore.AckMessage, error) {
	msg := eventstore.AckMessage{}
	err := ws.conn.ReadJSON(&msg)

	if err != nil {
		return msg, err
	}

	return msg, nil
}

func (ws *websocketConnection) WriteAckMessage(msg eventstore.AckMessage) error {
	err := ws.conn.WriteJSON(msg)

	if err != nil {
		return err
	}

	return nil
}

func (ws *websocketConnection) Close() error {
	return ws.conn.Close()
}

type websocketConnectionsMap struct {
	sync.RWMutex
	conns map[string]*websocketConnection
	upgrader *websocket.Upgrader
}

func (me *websocketConnectionsMap) Get(name string) (eventstore.NetworkConnection, error) {
	me.RLock()
	defer me.RUnlock()

	conn, connExists := me.conns[name]

	if ! connExists {
		return &websocketConnection{}, errors.New("Connection " + name + " does not exist")
	}

	return conn, nil
}

func (me *websocketConnectionsMap) Upgrade(name string, w http.ResponseWriter, r *http.Request) (*websocketConnection, error) {
	me.Lock()
	defer me.Unlock()

	conn := &websocketConnection{}

	// @todo check already exists and throw error
	ws, err := me.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return conn, err
	}

	conn.conn = ws

	me.conns[name] = conn

	return conn, nil
}

func (me *websocketConnectionsMap) Dial(name string, url string, headers http.Header) (*websocketConnection, error) {
	me.Lock()
	defer me.Unlock()

	conn := &websocketConnection{}

	ws, _, err := websocket.DefaultDialer.Dial(url, headers)

	if err != nil {
		return conn, err
	}

	conn.conn = ws

	me.conns[name] = conn

	return conn, nil
}

func CreateWebsocketConnectionsMap() *websocketConnectionsMap {
	wcm := &websocketConnectionsMap{
		upgrader: &websocket.Upgrader{
			// @todo seriously consider implications of this
			CheckOrigin: func(r *http.Request) bool { return true },
		},
		conns: make(map[string]*websocketConnection),
	}

	return wcm
}