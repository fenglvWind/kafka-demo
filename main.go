package main

import (
	"fmt"
	"github.com/fenglvWind/kafka-demo/producer"

)

func main() {
	//topicName := "test2"
	address := "127.0.0.1:9092"
	fmt.Println(producer.Producer.ListTopic(address))
	fmt.Println(producer.Producer.DeleteAllTopic(address))
	fmt.Println(producer.Producer.ListTopic(address))

	//err := producer.Producer.CreateTopic(topicName,address,5,1)
	//if err != nil {
	//	panic(err)
	//}
	//wg := &sync.WaitGroup{}
	//wg.Add(2)
	//go func() {
	//	for i := 0; i < 100; i++ {
	//		message := strconv.Itoa(i)+ "-->" + strconv.Itoa(int(time.Now().Unix()))
	//		err := producer.Producer.Send(topicName,message,address)
	//		if err != nil {
	//			fmt.Printf("--- Send message err:%v\n",err)
	//		} else {
	//			fmt.Printf("--- Send message[%v] success\n",message)
	//		}
	//	}
	//	wg.Done()
	//}()
	//go func() {
	//
	//	consumer.Consumer.ReceiveManual(topicName,address)
	//	wg.Done()
	//}()
	//wg.Wait()
	//fmt.Println("执行完成")

}
