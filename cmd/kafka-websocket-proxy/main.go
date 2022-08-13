package main

import (
	"context"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"net/http"
	"sync"
)

var (
	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}
	locker = make(map[string]sync.Mutex)
)

func reader(conn *websocket.Conn, clientName, topicName string) {
	mutex := locker[clientName]
	seeds := []string{
		"localhost:9092",
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup(clientName),
		kgo.ConsumeTopics(topicName),
	)
	if err != nil {
		mutex.Lock()
		conn.WriteJSON(struct {
			Error string `json:"error"`
		}{
			Error: err.Error(),
		})
		mutex.Unlock()
		return
	}
	defer func() {
		cl.Close()
		conn.Close()
	}()

	go func() {
		for {
			fetches := cl.PollFetches(context.TODO())
			if errs := fetches.Errors(); len(errs) > 0 {
				mutex.Lock()
				conn.WriteJSON(struct {
					Error []kgo.FetchError `json:"fetch_error"`
				}{
					Error: errs,
				})
				mutex.Unlock()
				log.Println(errs)
				return
			}
			iter := fetches.RecordIter()
			for !iter.Done() {
				record := iter.Next()
				//conn.WriteMessage(1, record.Value)
				mutex.Lock()
				conn.WriteJSON(struct {
					Message string `json:"message"`
					CG      string `json:"consumer_group_name"`
					Topic   string `json:"topic_name"`
					Value   string `json:"value"`
				}{
					Message: "received from kafka",
					CG:      clientName,
					Topic:   topicName,
					Value:   string(record.Value),
				})
				mutex.Unlock()
			}
		}
	}()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			//log.Println(err)
			conn.WriteJSON(struct {
				Error string `json:"error"`
			}{
				Error: err.Error(),
			})
			return
		}
		//log.Println("received from ws client", messageType, string(message))

		record := &kgo.Record{
			Topic: topicName,
			Value: message,
		}
		cl.Produce(context.TODO(), record, func(nrecord *kgo.Record, err error) {
			if err != nil {
				//log.Printf("record had a produce error: %v\n", err)
				mutex.Lock()
				conn.WriteJSON(struct {
					Error string `json:"error"`
				}{
					Error: err.Error(),
				})
				mutex.Unlock()
			} else {
				mutex.Lock()
				conn.WriteJSON(struct {
					Message       string `json:"message"`
					Topic         string `json:"topic_name"`
					Timestamp     int64  `json:"timestamp"`
					Partition     int32  `json:"partition"`
					ProducerEpoch int16  `json:"producer_epoch"`
					ProducerID    int64  `json:"producer_id"`
					LeaderEpoch   int32  `json:"leader_epochEpoch"`
					Offset        int64  `json:"offset"`
				}{
					Message:       "produced message",
					Topic:         topicName,
					Timestamp:     nrecord.Timestamp.Unix(),
					Partition:     nrecord.Partition,
					ProducerEpoch: nrecord.ProducerEpoch,
					ProducerID:    nrecord.ProducerID,
					LeaderEpoch:   nrecord.LeaderEpoch,
					Offset:        nrecord.Offset,
				})
				mutex.Unlock()
			}
		})

		//if err := conn.WriteMessage(messageType, message); err != nil {
		//	log.Println(err)
		//	return
		//}
	}
}

func wsEndpoint(w http.ResponseWriter, r *http.Request) {
	upgrader.CheckOrigin = func(r *http.Request) bool { return true }
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}

	params := mux.Vars(r)
	client_name := params["name"]
	topic_name := params["topic"]

	if client_name == "" || topic_name == "" {
		w.WriteHeader(400)
		return
	}
	locker[client_name] = sync.Mutex{}

	ws.WriteJSON(struct {
		Message string `json:"message"`
		CG      string `json:"consumer_group_name"`
		Topic   string `json:"topic_name"`
	}{
		Message: "Starting consume",
		CG:      client_name,
		Topic:   topic_name,
	})

	reader(ws, client_name, topic_name)
}

func main() {
	rtr := mux.NewRouter()
	rtr.HandleFunc("/ws/{name:[a-z]+}/{topic:[a-z]+}/", wsEndpoint)
	http.Handle("/", rtr)
	bindAddress := ":8080"
	log.Println("binding to", bindAddress)
	log.Fatal(http.ListenAndServe(bindAddress, nil))
}
