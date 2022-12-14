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
}

//创建kafka连接
func (k *producer) Conn(address string) (*kafka.Conn, error) {
	conn, err := kafka.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return nil, err
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return nil, err
	}
	return controllerConn, nil
}
//创建topic
func (k *producer) CreateTopic(topicName,address string,partitions,replication int ) error {
	conn, err := k.Conn(address)
	if err != nil {
		return err
	}
	defer conn.Close()
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

	err = conn.CreateTopics(topicConfig)
	if err != nil {

		log.Fatal(err)
	}
	return nil
}
//查询出集群中所有自己创建的topic
func (k *producer) ListTopic(address string) ([]string, error) {

	res := make([]string, 0)
	conn, err := k.Conn(address)
	if err != nil {
		return nil,err
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions()
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
func (k *producer) DeleteTopic(topicName,address string ) error {
	conn, err := k.Conn(address)
	if err != nil {
		return err
	}
	defer conn.Close()

	return  conn.DeleteTopics(topicName)
}
//判断topic是否存在集群中
func (k *producer) IsTopicExistence(topicName,address string ) (error){

	topicList,err := k.ListTopic(address)
	if err != nil {
		return  err
	}
	if len(topicList) ==0 {
		return nil
	}
	conn,err := k.Conn(address)
	if err != nil {
		return  err
	}
	defer conn.Close()
	return conn.DeleteTopics(topicList...)
}

//判断topic是否存在集群中
func (k *producer) DeleteAllTopic(address string ) ([]error,error){
	errList:= make([]error,0)
	topicList,err := k.ListTopic(address)
	if err != nil {
		return  errList,err
	}
	if len(topicList) ==0 {
		return errList,nil
	}
	for _,v:=range topicList {
		err := k.DeleteTopic(v,address)
		if err != nil {
			errList = append(errList,err)
		}
	}
	return errList,nil
}