package client

import (
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

type MClient struct {
	addr        string //
	connTimeout time.Duration
	soTimeout   time.Duration
}

type Client struct {
	Trans  *thrift.TSocket
	Client *thrift.TStandardClient
}

var (
	defaultStrictRead  = false
	defaultStrictWrite = false
)

func NewMClient(addr string, connTimeout, soTimeout time.Duration) *MClient {
	return &MClient{addr: addr, connTimeout: connTimeout, soTimeout: soTimeout}
}

func (mc *MClient) Client(serviceName string) (*Client, error) {
	trans, err := thrift.NewTSocketTimeout(mc.addr, mc.connTimeout, mc.soTimeout)
	if err != nil {
		return nil, err
	}
	if err := trans.Open(); err != nil {
		return nil, err
	}

	c := &Client{Trans: trans}

	protocol := thrift.NewTBinaryProtocolConf(trans, &thrift.TConfiguration{
		TBinaryStrictRead:  &defaultStrictRead,
		TBinaryStrictWrite: &defaultStrictWrite,
	})
	p := thrift.NewTMultiplexedProtocol(protocol, serviceName)
	c.Client = thrift.NewTStandardClient(p, p)
	return c, nil

}

func (mc *MClient) Client2(serviceName string, p thrift.TProtocol) (*Client, error) {
	trans, err := thrift.NewTSocketTimeout(mc.addr, mc.connTimeout, mc.soTimeout)
	if err != nil {
		return nil, err
	}
	if err := trans.Open(); err != nil {
		return nil, err
	}

	c := &Client{Trans: trans}
	c.Client = thrift.NewTStandardClient(p, p)

	return c, nil
}
