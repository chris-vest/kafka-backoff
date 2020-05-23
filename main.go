package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	backoff "github.com/jpillora/backoff"
	kafka "github.com/segmentio/kafka-go"
)

type connector struct {
	name          string
	taskID        int
	taskTrace     string
	restart       bool
	lastRestart   time.Time
	restartCount  int
	lastBackoff   time.Time
	backoffCount  int
	pendingAction string
}

func main() {
	// // make a writer that produces to topic-A, using the least-bytes distribution
	// w := kafka.NewWriter(kafka.WriterConfig{
	// 	Brokers:  []string{"192.168.99.100:9092"},
	// 	Topic:    "topic-A",
	// 	Balancer: &kafka.LeastBytes{},
	// })

	// err := w.WriteMessages(context.Background(),
	// 	kafka.Message{
	// 		Key:   []byte("connectorA-1"),
	// 		Value: []byte("restart"),
	// 	},
	// 	kafka.Message{
	// 		Key:   []byte("connectorA-3"),
	// 		Value: []byte("restart"),
	// 	},
	// 	kafka.Message{
	// 		Key:   []byte("connectorC-2"),
	// 		Value: []byte("restart"),
	// 	},
	// )

	// if err != nil {
	// 	log.Fatalln(err)
	// }

	// w.Close()

	connectors := []connector{
		{
			name:         "connectorA",
			taskID:       1,
			restart:      true,
			restartCount: 3,
			backoffCount: 0,
		},
		{
			name:         "connectorA",
			taskID:       2,
			restart:      false,
			restartCount: 0,
			backoffCount: 4,
		},
		{
			name:         "connectorB",
			taskID:       2,
			restart:      true,
			restartCount: 0,
			backoffCount: 5,
		},
		{
			name:         "connectorC",
			taskID:       2,
			restart:      true,
			restartCount: 0,
			backoffCount: 6,
		},
	}

	state := reader()

	calculate(connectors, state)
}

func reader() []kafka.Message {
	// make a new reader that consumes from topic-A, partition 0, at offset 42
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"192.168.99.100:9092"},
		Topic:     "topic-A",
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})

	state := []kafka.Message{}

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at offset %d: %s = %s // time: %v\n", m.Offset, string(m.Key), string(m.Value), m.Time)

		state = append(state, m)

		lag, err := r.ReadLag(context.Background())
		if err != nil {
			break
		}

		if lag == 0 {
			break
		}
	}

	r.Close()

	return state
}

func calculate(connectors []connector, state []kafka.Message) {

	for i := range state {
		split := strings.Split(string(state[i].Key), "-")

		connectorName := split[0]
		taskID, _ := strconv.Atoi(split[1])

		for j := range connectors {
			if connectors[j].name == connectorName && connectors[j].taskID == taskID {
				switch action := string(state[i].Value); action {
				case "restart":
					connectors[j].lastRestart = state[i].Time
					connectors[j].restartCount++
				case "backoff":
					connectors[j].lastBackoff = state[i].Time
					connectors[j].backoffCount++
				}
			}
		}
	}

	for i := range connectors {
		if connectors[i].lastRestart.After(connectors[i].lastBackoff) {
			log.Println("last restart AFTER last backoff")
			connectors[i].pendingAction = "backoff"
		} else {
			log.Println("last backoff AFTER last restart")
			connectors[i].pendingAction = "restart"
		}
	}

	log.Printf("%v", connectors)

	b := &backoff.Backoff{
		Min:    2 * time.Hour,
		Max:    24 * time.Hour,
		Factor: 2,
		Jitter: false,
	}

	var nextBackOff time.Duration

	for i := range connectors {
		for j := 0; j < connectors[i].restartCount; j++ {
			nextBackOff = b.Duration()
		}
		if time.Now().Before(connectors[i].lastRestart.Add(nextBackOff)) {
			fmt.Println("time.now() is before the last restart + backoff!\ntoo early to restart Connector again...")
		} else {
			fmt.Println("time.now() is after the last restart + backoff!\nsafe to restart Connector!")
		}
	}
}
