package server

import (
	"crypto/tls"
	"errors"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

var (
	defaultBufferSize  = 8192
	defaultStrictRead  = false
	defaultStrictWrite = false
)

type TServer struct {
	addr          string
	clientTimeout time.Duration
	tlsConfig     *tls.Config

	transportFactory thrift.TTransportFactory
	protocolFactory  thrift.TProtocolFactory
	processor        thrift.TProcessor
	Server           *thrift.TSimpleServer
}

func NewTServer(addr string) *TServer {
	pf := thrift.NewTBinaryProtocolFactoryConf(&thrift.TConfiguration{
		TBinaryStrictRead:  &defaultStrictRead,
		TBinaryStrictWrite: &defaultStrictWrite,
	})

	tf := thrift.NewTBufferedTransportFactory(defaultBufferSize)
	p := thrift.NewTMultiplexedProcessor()

	return NewTServer6(tf, pf, p, addr, 0, nil)
}

func NewTServer6(tf thrift.TTransportFactory, pf thrift.TProtocolFactory, p thrift.TProcessor, addr string, clientTimeout time.Duration, tlsConfig *tls.Config) *TServer {
	return &TServer{
		addr:             addr,
		clientTimeout:    clientTimeout,
		transportFactory: tf,
		protocolFactory:  pf,
		processor:        p,
		tlsConfig:        tlsConfig,
	}
}

func (s *TServer) RegisterProcessor(name string, processor thrift.TProcessor) error {
	p, ok := s.processor.(*thrift.TMultiplexedProcessor)
	if !ok {
		return errors.New("processor is not TMultiplexedProcessor.")
	}
	p.RegisterProcessor(name, processor)
	return nil
}

func (s *TServer) Serve() error {
	var transport thrift.TServerTransport
	var err error

	if s.tlsConfig != nil {
		transport, err = thrift.NewTSSLServerSocketTimeout(s.addr, s.tlsConfig, s.clientTimeout)
	} else {
		transport, err = thrift.NewTServerSocketTimeout(s.addr, s.clientTimeout)
	}

	if err != nil {
		return err
	}

	s.Server = thrift.NewTSimpleServer4(s.processor, transport, s.transportFactory, s.protocolFactory)

	return s.Server.Serve()
}

func (s *TServer) Stop() error {
	if s.Server == nil {
		return errors.New("Server not run.")
	}
	return s.Server.Stop()
}
