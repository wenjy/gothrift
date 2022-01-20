package main

import (
	"context"
	"fmt"
	"thriftgos/test1"
	"thriftgos/test2"

	"github.com/wenjy/gothrift/server"
)

func main() {
	addr := "localhost:9090"
	handler1 := newTest1Handler()
	handler2 := newTest2Handler()

	tServer := server.NewTServer(addr)
	tServer.RegisterProcessor("test1", test1.NewTest1Processor(handler1))
	tServer.RegisterProcessor("test2", test2.NewTest2Processor(handler2))
	err := tServer.Serve()
	if err != nil {
		fmt.Println(err)
	}
}

type test1Handler struct {
}

func newTest1Handler() *test1Handler {
	return &test1Handler{}
}

func (h *test1Handler) Hello1(ctx context.Context) (r string, err error) {
	return "test1->hello1", nil
}

func (h *test1Handler) Hello2(ctx context.Context) (r string, err error) {
	return "test1->hello2", nil
}

type test2Handler struct {
}

func newTest2Handler() *test2Handler {
	return &test2Handler{}
}

func (h *test2Handler) Hello1(ctx context.Context) (r string, err error) {
	return "test2->hello1", nil
}

func (h *test2Handler) Hello2(ctx context.Context) (r string, err error) {
	return "test2->hello2", nil
}
