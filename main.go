package main

import (
	"log"
	"time"

	"math/rand"

	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/Shopify/sarama"
)

func main() {

	var topic = "user-rank-input"
	var userId = "66"

	conf := sarama.NewConfig()
	conf.Version = sarama.MaxVersion
	conf.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, conf)
	if err != nil {
		log.Fatal(err)
	}

	rand.Seed(time.Now().Unix())

	// var format = "2006-01-02 15:04:05"
	for {
		var timestamp = time.Now().Add(-time.Second * time.Duration(rand.Intn(50)))
		var vt = timestamp.Truncate(10 * time.Second)

		_, _, err := producer.SendMessage(&sarama.ProducerMessage{
			Topic:     topic,
			Key:       sarama.StringEncoder(userId),
			Value:     sarama.ByteEncoder(float64ToByte(rand.Float64())),
			Timestamp: timestamp,
		})
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("send msg to window %d:%d\n", vt.Hour(), vt.Second())
		time.Sleep(time.Second)
	}

}

func float64ToByte(f float64) []byte {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, f)
	if err != nil {
		fmt.Println("binary.Write failed:", err)
	}
	return buf.Bytes()
}
