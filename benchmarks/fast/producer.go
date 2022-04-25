package pblh

import (
	"bytes"
	"log"
	"os"
	"strconv"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Producer struct {
	producer *kafka.Producer
	producerConfig *kafka.ConfigMap
	channel chan kafka.Event

	workerCount int64
	closed bool
	offset int64

	topic string
	totalFiles int
	partition int32
	partitions int32
}

func NewProducer(conf *kafka.ConfigMap, topic string, totalFiles int) (Producer, error) {
	producer, err := kafka.NewProducer(conf)
	if err != nil {
		return Producer{}, err
	}

	meta, err := producer.GetMetadata(&topic, false, 2000)
	if err != nil {
		return Producer{}, err
	}

	p := Producer {
		producer: producer,
		producerConfig: conf,

		workerCount: 0,
		closed: false,
		offset: 0,

		channel: make(chan kafka.Event, 10000),
		topic: topic,
		totalFiles: totalFiles,
		partition: 0,
		partitions: int32(len(meta.Topics[topic].Partitions)),
	}

	log.Println("partitions: ", p.partitions)
	return p, nil
}

func (p *Producer) Produce(contents []byte) error {
	err := p.producer.Produce(
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic: &p.topic,
				Partition: p.partition,
			},
			Value: contents,
		}, p.channel)
	p.partition = (p.partition + 1) % p.partitions
	return err
}

func (p *Producer) Flush(timeoutMs int) int {
	return p.producer.Flush(timeoutMs)
}

func (p *Producer) Renew() error {
	p.Close()

	producer, err := kafka.NewProducer(p.producerConfig)
	if err != nil {
		return err
	}

	p.producer = producer
	p.channel = make(chan kafka.Event, 10000)
	p.closed = false
	p.partition = 0
	return nil
}

func (p *Producer) Close() {
	if p.closed {
		return
	}

	select {
	case _, open := <-p.channel:
		if open {
			close(p.channel)
			// group these so we know when the kafka producer is closed, too
			p.producer.Close()
		}
	default:
		log.Println("channel already closed")
	}

	p.closed = true
}

func (p *Producer) SyncProducer() {
	var sync_topic string = "sync_" + p.topic

	sampleContents, _ := readSampleFile()

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "claude:9093",
		"group.id": "producer-sync",
	})
	if err != nil {
		log.Fatal("error creating sync consumer:", err)
	}

	log.Printf("subscribing to %s at offset %d", sync_topic, p.offset)

	offset, err := kafka.NewOffset(p.offset)
	if err != nil {
		log.Fatal("bad offset", p.offset, "- error:", err)
	}
	partitions := []kafka.TopicPartition{{
		Topic: &sync_topic,
		Partition: 0,
		Offset: offset,
	}}
	p.offset += 1
	consumer.Assign(partitions)

	p.workerCount = 0
	syncedWorkers := make([]int64, 0)
	log.Println("waiting for sync messages")
	for i := 0; i < 2; i++ {
		for {
			msg, err := consumer.ReadMessage(-1)
			if err != nil {
				log.Fatal("error polling sync messages")
			}
			p.offset += 1

			if bytes.Equal(msg.Key, []byte("size")) {
				s := string(msg.Value)
				p.workerCount, err = strconv.ParseInt(s, 10, 64)
				if err != nil {
					log.Fatal("error parsing horovod size:", err)
				}
				log.Println("cluster size:", p.workerCount)
			} else if bytes.Equal(msg.Key, []byte("consumer")) {
				s := string(msg.Value)
				worker, err := strconv.ParseInt(s, 10, 64)
				if err != nil {
					log.Fatal("error parsing worker rank:", err)
				}
				log.Println("sync from worker", worker)
				syncedWorkers = append(syncedWorkers, worker)
			} else {
				if msg.Key != nil {
					log.Printf("unknown msg key '%s'\n", msg.Key)
				}
			}
			log.Println("got", len(syncedWorkers), "want", p.workerCount)
			if p.workerCount != 0 && int64(len(syncedWorkers)) == p.workerCount {
				log.Println(p.workerCount, "messages received")
				break
			}
		}
		log.Println("finished sync", i+1, "of 2")
		for i := 0; int64(i) < p.workerCount; i++ {
			p.Produce(sampleContents)
		}
		syncedWorkers = make([]int64, 0)
	}

	consumer.Close()

	log.Println("sending sync")
	for i := int64(0); i < p.workerCount; i++ {
		p.ProduceSyncMsg()
	}
	p.Flush(1000)
}

func (p *Producer) ProduceNSamples(n int) {
	sampleContents, fileSize := readSampleFile()

	log.Printf("producing messages to topic '%s'\n", p.topic)
	start := time.Now()
	for i := n; i > 0; i-- {
		// the short message is 109 bytes long
		//p.Produce([]byte("this is a short message. a real one might contain a URI. it's about a hundred bytes long, give or take a bit."))
		p.Produce(sampleContents)

		if i % cap(p.channel) == 0 {
			for p.Flush(1000) > 0 {
			}
		}
	}

	p.Flush(15*1000)
	log.Println("done. sent", p.totalFiles, fileSize, "byte files in", time.Since(start))
}

func (p *Producer) ProduceSamples() {
	p.ProduceNSamples(p.totalFiles)
}

func (p *Producer) ProduceSyncMsg() error {
	sync_topic := "sync_" + p.topic

	p.offset += 1
	return p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &sync_topic,
			Partition: kafka.PartitionAny,
		},
		Key: []byte("producer"),
		Value: []byte("topic ready for consumption"),
	}, p.channel)
}

func readSampleFile() ([]byte, int64) {
	fileName, err := os.Readlink("/home/samit1/kafka/test/data/umbc/umbcCHM160112.l1.20200703.1955.nc")
	if err != nil {
		log.Fatal("error reading link to sample file:", err)
	}

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal("error opening sample file:", err)
	}


	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatal("error reading sample file metadata:", err)
	}
	fileSize := fileInfo.Size()

	contents := make([]byte, fileSize)
	n, err := file.Read(contents)
	if err != nil {
		log.Fatal("error reading sample file contents:", err)
	}
	if int64(n) != fileSize {
		log.Fatal("fileSize mismatch! expected", fileSize, "got", n)
	}

	return contents, fileSize
}

func (p *Producer) HandleProducerEvents() {
	remaining := p.totalFiles
	for e := range p.channel {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				log.Printf("delivery failed: %v\n", ev.TopicPartition)
			} else {
				remaining--;
				if remaining % 5000 == 0 {
					log.Printf("%d left\n", remaining)
				}
			}
		}
	}
}
