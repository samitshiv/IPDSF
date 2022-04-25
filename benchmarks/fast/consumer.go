package pblh

import (
	"errors"
	"log"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

type Consumer struct {
	consumer *kafka.Consumer

	Conf ConsumerConf
}

type ConsumerConf struct {
	Topic string
	Size int
	Rank int
	Hostname string
}

func (c *Consumer) handleRebalance(_ *kafka.Consumer, ev kafka.Event) error {
	log.Printf("%s/%d: rebalance, seeking to beginning...", c.Conf.Hostname, c.Conf.Rank)
	c.SeekToBeginning()
	return nil
}

func NewConsumer(kafkaConf *kafka.ConfigMap, conf ConsumerConf) (Consumer, error) {
	consumer, err := kafka.NewConsumer(kafkaConf)
	if err != nil {
		return Consumer{}, err
	}

	c := Consumer {
		consumer: consumer,
		Conf: conf,
	}

	consumer.Subscribe(conf.Topic, c.handleRebalance)

	return c, nil
}

func (c *Consumer) ConsumeSamples(doneChannel chan int64) {
	var count int = 0
	var allCount int64 = 0
	var totalSize int64 = 0

	log.Printf("%s/%d: reading initial message", c.Conf.Hostname, c.Conf.Rank)
	c.consumer.ReadMessage(time.Second * 5)
	c.SeekToBeginning()
	log.Printf("%s/%d: sleeping before start", c.Conf.Hostname, c.Conf.Rank)
	time.Sleep(time.Second * 10)

	start := time.Now()
	consecutiveTimeouts := 0

	for {
		msg, err := c.consumer.ReadMessage(time.Millisecond * 100)
		if err != nil {
			var kafkaErr kafka.Error
			if errors.As(err, &kafkaErr) {
				if kafkaErr.Code() == kafka.ErrTimedOut {
					consecutiveTimeouts += 1
					if consecutiveTimeouts == 100 {
						log.Printf("%s/%d: stalled for 100 requests at count %d!", c.Conf.Hostname, c.Conf.Rank, allCount)
					}
					if consecutiveTimeouts == 1000 {
						log.Printf("%s/%d: stalled for 1000 requests. quitting", c.Conf.Hostname, c.Conf.Rank)
						break
					}
				} else {
					log.Println("%s/%d: error consuming:", c.Conf.Hostname, c.Conf.Rank, err)
				}
			} else {
				log.Println("%s/%d: unknown error:", c.Conf.Hostname, c.Conf.Rank, err)
			}
			continue
		}

		if consecutiveTimeouts >= 100 {
			log.Printf("%s/%d: no longer stalled", c.Conf.Hostname, c.Conf.Rank)
		}
		consecutiveTimeouts = 0

		//log.Printf("%d byte msg on %s", len(msg.Value), msg.TopicPartition)
		count += 1
		allCount += 1
		totalSize += int64(len(msg.Value))

		if time.Since(start).Seconds() >= 120 {
			break
		}

		if count >= 6000 / (c.Conf.Size + 1) {
			count = 0
			c.SeekToBeginning()
		}
	}

	c.SeekToBeginning()
	c.consumer.Close()

	doneChannel <- allCount
}

func (c *Consumer) SeekToBeginning() {
	partitions, err := c.consumer.Assignment()
	if err != nil {
		log.Println("error fetching consumer assignments:", err)
	}

	partNumbers := make([]int32, len(partitions))
	for i, partition := range partitions {
		partNumbers[i] = partition.Partition
		partition.Offset = kafka.OffsetBeginning
		c.consumer.Seek(partition, 0)
	}

	//log.Printf("%s/%d: seeked to beginning on: %d", c.Conf.Hostname, c.Conf.Rank, partNumbers)
}
