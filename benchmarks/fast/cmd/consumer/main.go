package main

import (
	"flag"
	"log"
	"os"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

	pblh "pblh/v2"
)

const totalFiles int64 = 500_000

func main() {
	consumersFlag := flag.Int("n", 0, "number of consumers to run, or by defauly, run 1, 2, 4, 8, 16, 32, 64, and 128 consumers consecutively")
	flag.Parse()

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "<unknown host>"
		log.Println("unable to read hostname:", err)
	}

	if *consumersFlag > 0 {
		files, seconds := runConsumers(*consumersFlag)
		log.Printf("RESULTS: %d consumers, %d files in %f seconds on %s", *consumersFlag, files, seconds, hostname)
		return
	}

	log.Println("NO consumer count set!! running tests on powers of 2 from 1 to 128")

	for i := 1; i <= 128; i *= 2 {
		files, seconds := runConsumers(i)
		log.Printf("RESULTS: %d consumers, %d files in %f seconds on %s", i, files, seconds, hostname)
	}
}

func runConsumers(consumers int) (int64, float64) {
	doneChannel := make(chan int64, consumers)

	start := time.Now()

	for i := 0; i < consumers; i++ {
		go createAndConsume(consumers, i, doneChannel)
	}

	var total int64 = 0
	for count := 0; count < consumers; count++ {
		total += <-doneChannel
	}

	elapsed := time.Since(start)
	log.Printf("took %d consumers %s to consume %d files", consumers, elapsed, total)
	return total, elapsed.Seconds()
}

func createAndConsume(size int, rank int, doneChannel chan int64) {
	var topic string = "VATech-Blacksburg"

	hostname, err := os.Hostname()
	if err != nil {
		hostname = "<unknown host>"
		log.Println("unable to read hostname:", err)
	}

	kafkaConfig := &kafka.ConfigMap{
		//"bootstrap.servers": "claude:9093,claude2:9093",
		"bootstrap.servers": "claude:9093",
		//"bootstrap.servers": "localhost:9093",
		"group.id": "pblh-" + topic,
		"auto.offset.reset": "earliest",
		"go.application.rebalance.enable": true,
	}

	consumerConfig := pblh.ConsumerConf{
		Topic: topic,
		Size: size * 2,
		Rank: rank,
		Hostname: hostname,
	}

	c, err := pblh.NewConsumer(kafkaConfig, consumerConfig)
	if err != nil {
		log.Fatal("error creating producer:", err)
		return
	}

	log.Printf("consuming with %d consumers...", size)
	c.ConsumeSamples(doneChannel)
	log.Printf("%s/%d: consumed.", hostname, rank)
}
