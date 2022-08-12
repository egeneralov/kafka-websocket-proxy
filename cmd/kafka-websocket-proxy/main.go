package main

import (
	"context"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"net/http"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

func reader(conn *websocket.Conn, clientName, topicName string) {
	seeds := []string{
		"localhost:9092",
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup(clientName),
		kgo.ConsumeTopics(topicName),
	)
	if err != nil {
		conn.WriteJSON(struct {
			Error string `json:"error"`
		}{
			Error: err.Error(),
		})
		return
	}
	defer cl.Close()

	go func() {
		for {
			fetches := cl.PollFetches(context.TODO())
			if errs := fetches.Errors(); len(errs) > 0 {
				conn.WriteJSON(struct {
					Error []kgo.FetchError `json:"fetch_error"`
				}{
					Error: errs,
				})
				fmt.Println(errs)
			}
			iter := fetches.RecordIter()
			for !iter.Done() {
				record := iter.Next()
				//fmt.Println(string(record.Value), "from an iterator!")
				//conn.WriteJSON(record)
				conn.WriteMessage(1, record.Value)
			}
		}
	}()

	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			//log.Println(err)
			conn.WriteJSON(struct {
				Error string `json:"error"`
			}{
				Error: err.Error(),
			})
			return
		}
		fmt.Println("received from ws client", messageType, string(message))

		record := &kgo.Record{
			Topic: topicName,
			Value: message,
		}
		cl.Produce(context.TODO(), record, func(_ *kgo.Record, err error) {
			if err != nil {
				//fmt.Printf("record had a produce error: %v\n", err)
				conn.WriteJSON(struct {
					Error string `json:"error"`
				}{
					Error: err.Error(),
				})
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
		fmt.Println(err)
		return
	}

	params := mux.Vars(r)
	client_name := params["name"]
	topic_name := params["topic"]

	//ws.WriteJSON(struct {
	//	Name  string `json:"msg"`
	//	Topic string `json:"topic"`
	//}{
	//	Name:  client_name,
	//	Topic: topic_name,
	//})

	reader(ws, client_name, topic_name)
}

func main() {
	rtr := mux.NewRouter()
	rtr.HandleFunc("/ws/{name:[a-z]+}/{topic:[a-z]+}/", wsEndpoint)
	http.Handle("/", rtr)
	bindAddress := ":8080"
	fmt.Println("binding to", bindAddress)
	log.Fatal(http.ListenAndServe(bindAddress, nil))
}
