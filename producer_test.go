package t_test

import (
	"fmt"
	"github.com/fenglvWind/kafka-demo/producer"
	"testing"
	"time"
)
const Address = "192.168.202.101:31184"


const TopicName = "test"

var Producer = producer.Producer

func init()  {
	p, err := producer.NewConn(Address)
	if err != nil {
		panic(err)
	}
	Producer = p
	fmt.Println("init kafka conn is success")
}
func TestCreateTopic(t *testing.T)  {
	fmt.Println(Producer.CreateTopic(TopicName,1,1))
}
func TestListTopic(t *testing.T)  {
	fmt.Println(Producer.ListTopic())
}

func TestSendMsg(t *testing.T)  {
	for  {
		fmt.Println(Producer.Send(TopicName,fmt.Sprint(time.Now().UnixMilli()),Address))
		//time.Sleep(1* time.Millisecond)
	}
}
func TestDeleteAllTopic(t *testing.T)  {
	fmt.Println(Producer.DeleteAllTopic())
}
func TestDeleteTopic(t *testing.T)  {
	fmt.Println(Producer.DeleteTopic(TopicName))
}
func TestIsTopicExistence(t *testing.T)  {
	fmt.Println(Producer.IsTopicExistence(TopicName))
}
