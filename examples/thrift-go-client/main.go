package main

import (
	"context"
	"fmt"
	"thriftgoc/test1"
	"thriftgoc/test2"
	"time"

	"github.com/wenjy/gothrift/client"
)

func main() {
	addr := "localhost:9090"

	mc := client.NewMClient(addr, time.Second, time.Second)

	c1, err := mc.Client("test1")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer c1.Trans.Close()
	client1 := test1.NewTest1Client(c1.Client)
	fmt.Println(client1.Hello1(context.Background()))
	fmt.Println(client1.Hello2(context.Background()))

	c2, err := mc.Client("test2")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer c2.Trans.Close()
	client2 := test2.NewTest2Client(c2.Client)
	fmt.Println(client2.Hello1(context.Background()))
	fmt.Println(client2.Hello2(context.Background()))
}
