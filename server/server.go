package server

import (
	"crypto/tls"
	"errors"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

type TServer struct {
	addr          string //
	clientTimeout time.Duration

	transportFactory thrift.TTransportFactory
	protocolFactory  thrift.TProtocolFactory
	processor        thrift.TProcessor

	tlsConfig *tls.Config

	Server *thrift.TSimpleServer
}

func NewTServer(tf thrift.TTransportFactory, pf thrift.TProtocolFactory, p thrift.TProcessor, addr string, clientTimeout time.Duration, tlsConfig *tls.Config) *TServer {
	return &TServer{
		transportFactory: tf,
		protocolFactory:  pf,
		processor:        p,
		addr:             addr,
		clientTimeout:    clientTimeout,
		tlsConfig:        tlsConfig,
	}
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
