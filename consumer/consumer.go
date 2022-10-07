package consumer

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

var Consumer = &consumer{}
type consumer struct {

}
func (c *consumer) NewConsumer(topicName string,address ...string) *kafka.Reader{
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:                address,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
		CommitInterval: time.Second, // flushes commits to Kafka every second
		//消费者名称
		GroupID:                "dddddddd",
		Topic:                  topicName,
		//消费的分区
		//Partition:              0,
		//健康检查时间间隔
		//HeartbeatInterval:      0,

		//设置无心跳时超出多少时间剔出消费组
		//SessionTimeout:         0,

	})
}

func (c *consumer) ReceiveAuto(topicName string,address ...string) error{
	r:= c.NewConsumer(topicName,address...)

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			break
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}
	defer func() {
		if err := r.Close(); err != nil {
			log.Fatal("failed to close reader:", err)
		}
	}()
	
	return nil
}

func (c *consumer) ReceiveManual(topicName string,address ...string) error{
	r:= c.NewConsumer(topicName,address...)

	ctx := context.Background()
	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			break
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		if err := r.CommitMessages(ctx, m); err != nil {
			log.Fatal("failed to commit messages:", err)
		}
	}
	defer func() {
		if err := r.Close(); err != nil {
			log.Fatal("failed to close reader:", err)
		}
	}()

	return nil
}