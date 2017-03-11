package main

import (
	"flag"
	"github.com/writepush-labs/eventor/server"
	log "github.com/writepush-labs/eventor/logging"
)

var VERSION string

/*func startStreamReplication(url string) {
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {

	}
	defer c.Close()
}

// lets deal with replication
	if len(*opts.Replicate) > 0 {
		es.StartReplication();

		// connect to all of the streams we are replicating
		// connect to activity stream
	}

*/

func main() {
	opts := &server.ServerOptions{}
	opts.Debug     = flag.Bool("prettylog", false, "Output pretty log")
	opts.Port      = flag.String("port", "9400", "Port to listen on")
	opts.DataPath  = flag.String("data", "./data", "Path to data")
	opts.Replicate = flag.String("replicate", "", "URL to replicate from")

	flag.Parse()

	logger := log.CreateLogger(*opts.Debug)

	logger.Info("Starting Eventor", log.String("version", VERSION))

	server.CreateServer(opts, logger)
}
