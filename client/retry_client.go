package client

import (
	"context"
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/wenjy/gothrift/pool"
)

// RetryClient 重写 thrift.TStandardClient 实现重试的客户端
type RetryClient struct {
	seqId        int32
	iprot, oprot thrift.TProtocol
	conn         *pool.Conn
	err          error

	transPool *TransPool

	serviceName string // use MultiplexedProtocol
	tConfig     *thrift.TConfiguration
}

//
func NewRetryClient(tp *TransPool, serviceName string, tConfig *thrift.TConfiguration) (rc *RetryClient, err error) {
	rc = &RetryClient{transPool: tp, serviceName: serviceName, tConfig: tConfig}
	err = rc.retryGetConn()
	return
}

// Send thrift.TStandardClient.Send
func (p *RetryClient) Send(ctx context.Context, oprot thrift.TProtocol, seqId int32, method string, args thrift.TStruct) error {
	// Set headers from context object on THeaderProtocol
	if headerProt, ok := oprot.(*thrift.THeaderProtocol); ok {
		headerProt.ClearWriteHeaders()
		for _, key := range thrift.GetWriteHeaderList(ctx) {
			if value, ok := thrift.GetHeader(ctx, key); ok {
				headerProt.SetWriteHeader(key, value)
			}
		}
	}

	if err := oprot.WriteMessageBegin(ctx, method, thrift.CALL, seqId); err != nil {
		return err
	}
	if err := args.Write(ctx, oprot); err != nil {
		return err
	}
	if err := oprot.WriteMessageEnd(ctx); err != nil {
		return err
	}
	return oprot.Flush(ctx)
}

// Recv thrift.TStandardClient.Recv
func (p *RetryClient) Recv(ctx context.Context, iprot thrift.TProtocol, seqId int32, method string, result thrift.TStruct) error {
	rMethod, rTypeId, rSeqId, err := iprot.ReadMessageBegin(ctx)
	if err != nil {
		return err
	}

	if method != rMethod {
		return thrift.NewTApplicationException(thrift.WRONG_METHOD_NAME, fmt.Sprintf("%s: wrong method name", method))
	} else if seqId != rSeqId {
		return thrift.NewTApplicationException(thrift.BAD_SEQUENCE_ID, fmt.Sprintf("%s: out of order sequence response", method))
	} else if rTypeId == thrift.EXCEPTION {
		// 原来是私有的，这里使用new替换
		exception := thrift.NewTApplicationException(thrift.UNKNOWN_APPLICATION_EXCEPTION, "")
		if err := exception.Read(ctx, iprot); err != nil {
			return err
		}

		if err := iprot.ReadMessageEnd(ctx); err != nil {
			return err
		}

		return exception
	} else if rTypeId != thrift.REPLY {
		return thrift.NewTApplicationException(thrift.INVALID_MESSAGE_TYPE_EXCEPTION, fmt.Sprintf("%s: invalid message type", method))
	}

	if err := result.Read(ctx, iprot); err != nil {
		return err
	}

	return iprot.ReadMessageEnd(ctx)
}

// Call 在这里实现重试
func (p *RetryClient) Call(ctx context.Context, method string, args, result thrift.TStruct) (thrift.ResponseMeta, error) {
	p.seqId++
	seqId := p.seqId

	var retry int

	for retry = 0; retry < 3; retry++ {
		if p.err = p.Send(ctx, p.oprot, seqId, method, args); p.err != nil {
			// 连接已关闭重新获取连接
			if connIsClose(p.err) {
				if err := p.retryGetConn(); err != nil {
					return thrift.ResponseMeta{}, err
				}

				p.seqId++
				seqId = p.seqId
				continue
			}
			break
		}

		// method is oneway
		if result == nil {
			break
		}

		p.err = p.Recv(ctx, p.iprot, seqId, method, result)
		break
	}

	var headers thrift.THeaderMap
	if hp, ok := p.iprot.(*thrift.THeaderProtocol); ok {
		headers = hp.GetReadHeaders()
	}

	p.transPool.PutConn(p.conn, p.err)
	return thrift.ResponseMeta{
		Headers: headers,
	}, p.err
}

// 重新获取连接
func (p *RetryClient) retryGetConn() (err error) {
	// 释放旧连接
	if p.conn != nil {
		p.transPool.PutConn(p.conn, p.err)
	}

	p.conn, err = p.transPool.GetConn()
	if err != nil {
		return
	}

	if p.serviceName != "" {
		protocol := thrift.NewTBinaryProtocolConf(p.conn.GetTSocket(), p.tConfig)
		mp := thrift.NewTMultiplexedProtocol(protocol, p.serviceName)
		p.iprot = mp
		p.oprot = mp
	} else {
		protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()
		p.oprot = protocolFactory.GetProtocol(p.conn.GetTSocket())
		p.iprot = protocolFactory.GetProtocol(p.conn.GetTSocket())
	}
	return nil
}
