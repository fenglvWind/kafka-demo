package t_test

import (
	"fmt"
	"github.com/fenglvWind/kafka-demo/producer"
	"github.com/segmentio/kafka-go"
	"testing"
)
const Address = "192.168.202.101:31184"
var Conn *kafka.Conn

const TopicName = "test"

func init()  {
	conn, err := producer.Producer.Conn(Address)
	if err != nil {
		panic(err)
	}
	Conn = conn
	fmt.Println("init kafka conn is success")
}
func TestCreateTopic(t *testing.T)  {
	fmt.Println(producer.Producer.CreateTopic(TopicName,1,1))
}
func TestListTopic(t *testing.T)  {
	fmt.Println(producer.Producer.ListTopic())
}

func TestSendMsg(t *testing.T)  {
	fmt.Println(producer.Producer.Send(TopicName,"test",Address))
}
func TestDeleteAllTopic(t *testing.T)  {
	fmt.Println(producer.Producer.DeleteAllTopic())
}
func TestDeleteTopic(t *testing.T)  {
	fmt.Println(producer.Producer.DeleteTopic(TopicName))
}
func TestIsTopicExistence(t *testing.T)  {
	fmt.Println(producer.Producer.IsTopicExistence(TopicName))
}
