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
	Conn *kafka.Reader
	MsgChannel chan kafka.Message
}

var (
	//自动提交消费成功offset的时间间隔（默认1s）
	commitInterval = 1 * time.Second
	//消息缓冲区大小（默认10000）
	msgBufferSize = 10000
	//从那个分区消费
	partition = 0
	//消费组名称
	GroupID = fmt.Sprintf("g_cjh_%v",time.Now().Unix())
	//代理中的字节数多少时取数据
	minBytes  =1
	//最大等待时间
	maxWait = 1 * time.Second
)

type ConsumerConfig struct {
	//主题名称
	TopicName string
	//kafka地址列表
	AddressList []string
	//代理中的字节数多少时取数据
	MinBytes int
	//从那个分区消费
	Partition int
	//消费组名称
	GroupID string
	//自动提交消费成功offset的时间间隔（默认1s）
	CommitInterval time.Duration
	//消息缓冲区大小（默认10000）
	MsgBufferSize int
	//最大等待时间
	MaxWait time.Duration
}
//创建消费者
//GroupID和Partition不能同时存在
func  NewConsumer(config *ConsumerConfig) (c *consumer){
	c = &consumer{}
	if config.CommitInterval == 0 {
		config.CommitInterval = commitInterval
	}
	if config.MinBytes == 0 {
		config.MinBytes = minBytes
	}
	if config.GroupID == "" {
		config.GroupID = GroupID
	}
	if config.MsgBufferSize == 0 {
		config.MsgBufferSize = msgBufferSize
	}
	if config.MaxWait == 0 {
		config.MaxWait = maxWait
	}
	conn :=  kafka.NewReader(kafka.ReaderConfig{
		Brokers:                config.AddressList,
		MinBytes:  config.MinBytes, // 10KB
		MaxBytes:  10e6, // 10MB
		CommitInterval: config.CommitInterval, // flushes commits to Kafka every second
		//消费者名称
		GroupID:                config.GroupID,
		Topic:                  config.TopicName,
		//消费的分区
		Partition:              config.Partition,
		//健康检查时间间隔
		//HeartbeatInterval:      0,

		//设置无心跳时超出多少时间剔出消费组
		//SessionTimeout:         0,
		//最大等待时间
		MaxWait: config.MaxWait,

	})
	c.Conn = conn
	c.MsgChannel = make(chan kafka.Message, config.MsgBufferSize)
	Consumer = c
	return
}

func (c *consumer) ReceiveAuto() (err error){

	for {
		m, err := c.Conn.ReadMessage(context.Background())
		if err != nil {
			break
		}
		topicPartitionOffset := fmt.Sprintf("%v/%v/%v",m.Topic, m.Partition, m.Offset)
		c.MsgChannel <- m
		fmt.Printf("message at topic/partition/offset/recv_time %v/%v: %s = %s\n", topicPartitionOffset,time.Now().Unix(), string(m.Key), string(m.Value))
	}
	//defer func() {
	//	if err = c.Conn.Close(); err != nil {
	//		log.Fatal("failed to close reader:", err)
	//	}
	//}()
	
	return
}

func (c *consumer) ReceiveManual() (err error){
	ctx := context.Background()
	for {
		m, err := c.Conn.FetchMessage(ctx)
		if err != nil {
			break
		}
		topicPartitionOffset := fmt.Sprintf("%v/%v/%v",m.Topic, m.Partition, m.Offset)
		fmt.Printf("message at topic/partition/offset/recv_time %v/%v: %s = %s\n", topicPartitionOffset,time.Now().Unix(), string(m.Key), string(m.Value))
		c.MsgChannel <- m
		if err := c.Conn.CommitMessages(ctx, m); err != nil {
			log.Fatal("failed to commit messages:", err)
			break
		}
	}
	//defer func() {
	//	if err := c.Conn.Close(); err != nil {
	//		log.Fatal("failed to close reader:", err)
	//	}
	//}()

	return
}