package main

import (
	"log"
	"flag"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

	pblh "pblh/v2"
)


func main() {
	//const totalFiles int = 550_000
	const totalFiles int = 25_000
	var topic string = "VATech-Blacksburg"
	//producerConfig := &kafka.ConfigMap{"bootstrap.servers": "claude:9093,claude2:9093"}
	producerConfig := &kafka.ConfigMap{"bootstrap.servers": "claude:9093"}

	countFlag := flag.Int("n", 0, "number of messages to produce")
	flag.Parse()

	p, err := pblh.NewProducer(producerConfig, topic, totalFiles)
	if err != nil {
		log.Fatal("error creating producer:", err)
		return
	}

	go p.HandleProducerEvents()

	if *countFlag > 0 {
		log.Println("producing", *countFlag, "samples...")
		p.ProduceNSamples(*countFlag)
		log.Println("produced", *countFlag, "samples...")
		p.Close()
		return
	}

	log.Println("producing...")
	p.ProduceSamples()
	log.Println("produced.")
	p.Close()
}

	/*
	for {
		log.Println("syncing with consumer")

		err = p.Renew()
		if err != nil {
			log.Fatal("error creating sync producer:", err)
			return
		}

		p.SyncProducer()
		p.Close()
	}
	*/
