package producer

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
	"net"
	"strconv"
	"strings"
	"time"
)

var Producer = &producer{}

type producer struct {
	KafkaConn *kafka.Conn
}

//创建kafka连接
func NewConn(address string) (c *producer,err error) {
	c = &producer{}
	conn, err := kafka.Dial("tcp", address)
	if err != nil {
		return
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return
	}
	c.KafkaConn = controllerConn
	return
}
//创建topic
func (k *producer) CreateTopic(topicName string,partitions,replication int ) error {

	if partitions == 0 {
		partitions = 1
	}
	if replication == 0 {
		replication = 1
	}
	topicConfig := kafka.TopicConfig{
			Topic:             topicName,
			NumPartitions:     partitions,
			ReplicationFactor: replication,
	}

	err := k.KafkaConn.CreateTopics(topicConfig)
	if err != nil {

		log.Fatal(err)
	}
	return nil
}
//查询出集群中所有自己创建的topic
func (k *producer) ListTopic() ([]string, error) {
	res := make([]string, 0)
	partitions, err := k.KafkaConn.ReadPartitions()
	if err != nil {
		return res, nil
	}

	m := map[string]struct{}{}

	for _, p := range partitions {
		if !strings.Contains(p.Topic, "__consumer_offsets") {
			if !strings.Contains(p.Topic, "_strimzi") {
				m[p.Topic] = struct{}{}
			}
		}

	}
	for k := range m {
		res = append(res, k)
	}
	return res, nil
}

//创建生产者
func (k *producer) NewProducer(topicName string, address ...string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(address...),
		Topic:    topicName,
		Balancer: kafka.CRC32Balancer{},
		//限制传递消息的尝试次数。 默认设置是最多尝试10次。
		MaxAttempts: 2,
		//在发送到之前限制请求的最大大小（消息条数）默认值是使用kafka默认值100条。
		BatchSize: 10,
		//在发送到之前限制请求的最大大小（以字节为单位）默认值是使用kafka默认值1MB。
		//BatchBytes: 0,
		//设置每次刷新数据到kafka的时间，默认1秒刷新一次
		BatchTimeout: time.Millisecond *100,
		//编写器执行的写入操作超时。默认为10秒。
		WriteTimeout: time.Second * 2,
		//是否需要kafka返回ack ，默认为0不需要
		RequiredAcks: kafka.RequireOne,
		//将此标志设置为true会导致WriteMessages方法从不阻塞。 设置为true忽略错误 //默认为false不忽略。
		//Async:       false,
		//Completion:  nil,
		Compression: kafka.Snappy,
		//Logger:      nil,
		//ErrorLogger: nil,
		//Transport:   nil,
		//是否自动创建主题
		AllowAutoTopicCreation: true,
	}

}
//发送消息
func (k *producer) Send(topicName string, message string,address ...string) error {
	w := k.NewProducer(topicName, address...)
	return w.WriteMessages(context.Background(), kafka.Message{
		Value: []byte(message),
	})
}
//删除topic
func (k *producer) DeleteTopic(topicName string ) error {
	return  k.KafkaConn.DeleteTopics(topicName)
}
//判断topic是否存在集群中
func (k *producer) IsTopicExistence(topicName string ) (res bool, err error){
	topicList,err := k.ListTopic()
	if err != nil {
		return
	}
	if len(topicList) ==0 {
		return
	}

	for _, name := range topicList {
		if name == topicName {
			res = true
			break
		}
	}
	return
}

//删除所有非系统的topic
func (k *producer) DeleteAllTopic() (err error){
	topicList,err := k.ListTopic()
	if err != nil {
		return
	}
	if len(topicList) ==0 {
		return nil
	}
	return k.KafkaConn.DeleteTopics(topicList...)

}