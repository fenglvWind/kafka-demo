package t_test

import (
	"fmt"
	"github.com/fenglvWind/kafka-demo/consumer"
	"testing"
)





//func init()  {
//	conn := consumer.Consumer.NewConsumer(TopicName,Address)
//
//	ConsumerConn = conn
//	fmt.Println("init kafka conn is success")
//}


func TestReceiveAuto(t *testing.T)  {
	c := consumer.NewConsumer(&consumer.ConsumerConfig{
		TopicName:   TopicName,
		AddressList: []string{Address},
	})
	fmt.Println(c.ReceiveAuto())
}

func TestReceiveManual(t *testing.T)  {
	c := consumer.NewConsumer(&consumer.ConsumerConfig{
		TopicName:   TopicName,
		AddressList: []string{Address},
	})
	fmt.Println(c.ReceiveManual())
}