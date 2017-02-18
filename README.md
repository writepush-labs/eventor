# Eventor
Eventstore implementation written in Go. 

## Project status
**WIP**. At the moment Eventor is used in production on a very limited set of non-critical projects and is still considered 
to be alpha/experimental software.

## Rationale
Eventor is useful in service-oriented setups where loose coupling between services is important. 
Also because Eventor utilises HTTP to transport messages it is trivial to plug new services - no extra libraries or learning new
protocols is required. 

## How does Eventor work?
Core concepts in Eventor are Events (surprise!), Streams and Subscriptions. Streams are just immutable, append-only series of Events.
Subscriptions are HTTP endpoints/webhooks that receive Events as soon as they arrive to Eventor. All streams are persisted on disk.
It is important to know that Subscriptions will receive Events in exact same sequential order they arrived in the first place.
Eventor uses HTTP codes to differentiate betwen succesful and unsuccesful delivery. 
When subscription is created on a pre-existing stream with a number of events, subscription will catchup right from the 
beginning of the stream. Eventor also tracks the position of last successfully delivered message for each subscription, no
external tracking is required. However it is up to application to process events in an idempotent way.

## Installation
Easiest way is to get a tiny Alpine based docker image:
```
docker run -it --rm -p 9400:9400 -v "$PWD/data":/writepush-labs/data writepushlabs/eventor
```
There is also a Mac OSX binary available for each release.

## HTTP API
TODO

## Web UI
TODO

## Storage backends
SQLite is used primarily for it's durability and various performance optimisation options.
Each stream is stored in a separate database file.

## License
Copyright (c) 2017 Writepush Labs

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
