package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Broker routes incoming PUT and GET requests to appropriate Worker depending
// on the queue name
type Broker struct {
	workers     map[string]*Worker // map of workers. Key is the name of queue
	getRequests chan *GetRequest   // channel with GetRequests came for the data
	mu          sync.Mutex
}

// Worker serves GetRequests, stores waiters (GetRequests with a timeout) and stores
// data queue.
type Worker struct {
	getRequests chan *GetRequest
	store       Storage
	waiters     Queue
}

// GetRequest is created in the http handler function on every GET request.
type GetRequest struct {
	QueueName   string              // requested queue name
	DataChannel chan string         // channel where to send requested data
	writer      http.ResponseWriter // http.ResponseWriter to reply
	timeout     int                 // timeout in milliseconds
	done        chan bool           // channels for control of GetRequest's state
	stopped     bool                // Indicates if the GetRequest active or not
	mu          sync.Mutex
}

// Worker's data storage
type Storage struct {
	data []string
	mu   sync.Mutex
}

// Worker's queue of GetRequests
type Queue struct {
	qu []*GetRequest
	mu sync.Mutex
}

// AddGetRequest adds every GetRequest to Broker.getRequests channel for subsequent
// processing
func (b *Broker) AddGetRequest(req *GetRequest) {
	b.getRequests <- req
}

// PutData is always called when a new data pair comes. Broker sends data to
// suitable worker depending on the queue name (data[0]). It creates a new
// Worker unless it exists
func (b *Broker) PutData(queue_name string, value string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.workers[queue_name]; !ok {
		w := makeWorker()
		b.workers[queue_name] = &w
		go w.Start()
	}
	b.workers[queue_name].Catch(value)
}

// Route is always called when a new GetRequest comes from the Broker.getRequests
// channel. It routes GetRequest to suitable Worker depending on the requested
// queue name. It creates a new Worker unless it exists
func (b *Broker) Route(req *GetRequest) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.workers[req.QueueName]; !ok {
		w := makeWorker()
		b.workers[req.QueueName] = &w
		go w.Start()
	}
	b.workers[req.QueueName].getRequests <- req
}

// Feed sends requested data to GetRequest.DataChannel channel and returns true. If
// the GetRequest is already stopped, Feed returns false
func (req *GetRequest) Feed(s string) bool {
	req.mu.Lock()
	defer req.mu.Unlock()
	if !req.stopped {
		req.DataChannel <- s
		return true
	}
	return false
}

// Stop sets GetRequest.stopped to true.
func (req *GetRequest) Stop() {
	req.mu.Lock()
	req.stopped = true
	req.mu.Unlock()
}

// Reply sends Data via http
func (req *GetRequest) Reply(s string) {
	req.writer.WriteHeader(http.StatusOK)
	req.writer.Write([]byte(s))
}

// Decline sends error 404 via http
func (req *GetRequest) Decline() {
	req.writer.WriteHeader(http.StatusNotFound)
	req.writer.Write(nil)
}

// Add adds GetRequest to the Queue
func (q *Queue) Add(req *GetRequest) {
	q.mu.Lock()
	q.qu = append(q.qu, req)
	q.mu.Unlock()
}

// A Worker constructor
func makeWorker() Worker {
	return Worker{
		getRequests: make(chan *GetRequest, 10),
		waiters:     Queue{qu: []*GetRequest{}},
		store:       Storage{data: []string{}},
	}
}

// Get returns true and data string if the data presents in storage.
// Otherwise it returns false and empty string
func (w *Worker) Get() (bool, string) {
	w.store.mu.Lock()
	defer w.store.mu.Unlock()
	if len(w.store.data) > 0 {
		s := w.store.data[0]
		w.store.data = w.store.data[1:]
		return true, s
	}
	return false, ""
}

// Catch sends a new data to a Worker. If trere is no GetRequests
// awaiting the data, it will be stored to Storage Worker.store
func (w *Worker) Catch(s string) {
	if w.ServeWaiter(s) {
		return
	}
	w.store.mu.Lock()
	w.store.data = append(w.store.data, s)
	w.store.mu.Unlock()
}

// ServeWaiter returns true if any pending GetRequest was successfully
// served. Otherwise it returns false
func (w *Worker) ServeWaiter(s string) bool {
	w.waiters.mu.Lock()
	defer w.waiters.mu.Unlock()
	for i := 0; i < len(w.waiters.qu); i++ {
		//		w.waiters.qu[i].PauseTimer()
		if !w.waiters.qu[i].Feed(s) {
			w.waiters.qu = append(w.waiters.qu[:i], w.waiters.qu[i+1:]...)
			i--
		} else {
			w.waiters.qu = append(w.waiters.qu[:i], w.waiters.qu[i+1:]...)
			return true
		}
	}
	return false
}

// Starts Worker to serve its GetRequests
func (w *Worker) Start() {
	for req := range w.getRequests {
		if ok, data := w.Get(); ok {
			req.DataChannel <- data
		} else if req.timeout > 0 {
			w.waiters.Add(req)
		} else {
			req.done <- true
		}
	}
}

func HandleRequest(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	url_path_slice := strings.Split(r.URL.Path, "/")
	queue_name := url_path_slice[len(url_path_slice)-1]
	// If queue name is empty, it is a bad request (400)
	if queue_name == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write(nil)
		return
	}
	r.ParseForm()

	if r.Method == "PUT" {
		if val, ok := r.Form["v"]; ok == true && val[0] != "" {
			br.PutData(queue_name, val[0])
		} else {
			w.WriteHeader(http.StatusBadRequest)
			w.Write(nil)
		}
		return
	}

	if r.Method == "GET" {
		var timer int
		if val, ok := r.Form["timeout"]; ok == true && val[0] != "" {
			if t, err := strconv.Atoi(val[0]); err == nil && t > 0 {
				timer = t * 1000
			}
		}
		me := &GetRequest{
			QueueName:   queue_name,
			DataChannel: make(chan string),
			timeout:     timer,
			writer:      w,
			done:        make(chan bool),
			stopped:     false,
		}
		// Send GetRequest to Broker's common channel for all GetRequests
		br.Route(me)
		// Start timeout countdown for the GetRequest
		ctx := context.Background()
		var cancel context.CancelFunc
		if me.timeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, time.Duration(me.timeout)*time.Millisecond)
			defer cancel()
		}
		// this select awaiting one of possible result of the GetRequest's processing
		select {
		case response := <-me.DataChannel: 		// in case of data received, reply 200 OK and send data
			me.Reply(response)
		case <-me.done: 				// in case of no data was received, reply 404
			me.Decline()
		case <-ctx.Done(): 				// in case of time is up, check DataChannel to avoid lost of the data
			select { 				// when data was alreday retrieved at the moment of expiration,
			case response := <-me.DataChannel: 	// then set GetRequest.stopped = true and reply 404
				me.Reply(response)
			default:
				me.Stop()
				me.Decline()
			}
		case <-r.Context().Done(): 			// in case of sudden disconnection, set GetRequest.stopped = true
			me.Stop()
		}
	}
}

var br Broker

func main() {
	// Broker:
	br = Broker{
		workers:     make(map[string]*Worker),
		getRequests: make(chan *GetRequest),
	}

	http.HandleFunc("/", HandleRequest)

	var ListenPort string
	flag.StringVar(&ListenPort, "port", "80", "Http port where to serve requests")
	flag.Parse()

	fmt.Println(http.ListenAndServe("localhost:"+ListenPort, nil).Error())
}
