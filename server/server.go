package server

import (
	"errors"
	"io"
	"log"
	"sync"
	"sync/atomic"

	"github.com/apache/thrift/lib/go/thrift"
)

type TServer struct {
	wg   sync.WaitGroup
	lock sync.Mutex

	processorFactory       thrift.TProcessorFactory
	serverTransport        thrift.TServerTransport
	inputTransportFactory  thrift.TTransportFactory
	outputTransportFactory thrift.TTransportFactory
	inputProtocolFactory   thrift.TProtocolFactory
	outputProtocolFactory  thrift.TProtocolFactory

	forwardHeaders []string

	closed int32
}

func NewTServer(pf thrift.TProcessorFactory, st thrift.TServerTransport, itf thrift.TTransportFactory, otf thrift.TTransportFactory, ipf thrift.TProtocolFactory, opf thrift.TProtocolFactory) *TServer {
	return &TServer{
		processorFactory:       pf,
		serverTransport:        st,
		inputTransportFactory:  itf,
		outputTransportFactory: otf,
		inputProtocolFactory:   ipf,
		outputProtocolFactory:  opf,
	}
}

// implement thrift.TServer interface
func (s *TServer) ProcessorFactory() thrift.TProcessorFactory {
	return s.processorFactory
}

// implement thrift.TServer interface
func (s *TServer) ServerTransport() thrift.TServerTransport {
	return s.serverTransport
}

// implement thrift.TServer interface
func (s *TServer) InputTransportFactory() thrift.TTransportFactory {
	return s.inputTransportFactory
}

// implement thrift.TServer interface
func (s *TServer) OutputTransportFactory() thrift.TTransportFactory {
	return s.outputTransportFactory
}

// implement thrift.TServer interface
func (s *TServer) InputProtocolFactory() thrift.TProtocolFactory {
	return s.inputProtocolFactory
}

// implement thrift.TServer interface
func (s *TServer) OutputProtocolFactory() thrift.TProtocolFactory {
	return s.outputProtocolFactory
}

// implement thrift.TServer interface
func (s *TServer) Listen() error {
	return s.serverTransport.Listen()
}

// Listen and AcceptLoop
func (s *TServer) Serve() error {
	if err := s.Listen(); err != nil {
		return err
	}
	s.AcceptLoop()
	return nil
}

func (s *TServer) AcceptLoop() error {
	for {
		closed, err := s.accept()
		if err != nil {
			return err
		}
		if closed != 0 {
			return nil
		}
	}
}

func (s *TServer) accept() (int32, error) {
	client, err := s.serverTransport.Accept()
	s.lock.Lock()
	defer s.lock.Unlock()

	if closed := atomic.LoadInt32(&s.closed); closed != 0 {
		return closed, nil
	}
	if err != nil {
		return 0, err
	}
	if client != nil {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if err := s.process(client); err != nil {
				log.Println("error processing request:", err)
			}
		}()
	}
	return 0, nil
}

func (p *TServer) process(client thrift.TTransport) (err error) {
	defer func() {
		err = treatEOFErrorsAsNil(err)
	}()

	processor := p.processorFactory.GetProcessor(client)
	inputTransport, err := p.inputTransportFactory.GetTransport(client)
	if err != nil {
		return err
	}
	inputProtocol := p.inputProtocolFactory.GetProtocol(inputTransport)
	var outputTransport thrift.TTransport
	var outputProtocol thrift.TProtocol

	// for THeaderProtocol, we must use the same protocol instance for
	// input and output so that the response is in the same dialect that
	// the server detected the request was in.
	headerProtocol, ok := inputProtocol.(*thrift.THeaderProtocol)
	if ok {
		outputProtocol = inputProtocol
	} else {
		oTrans, err := p.outputTransportFactory.GetTransport(client)
		if err != nil {
			return err
		}
		outputTransport = oTrans
		outputProtocol = p.outputProtocolFactory.GetProtocol(outputTransport)
	}

	if inputTransport != nil {
		defer inputTransport.Close()
	}
	if outputTransport != nil {
		defer outputTransport.Close()
	}
	for {
		if atomic.LoadInt32(&p.closed) != 0 {
			return nil
		}

		ctx := thrift.SetResponseHelper(
			defaultCtx,
			thrift.TResponseHelper{
				THeaderResponseHelper: thrift.NewTHeaderResponseHelper(outputProtocol),
			},
		)
		if headerProtocol != nil {
			// We need to call ReadFrame here, otherwise we won't
			// get any headers on the AddReadTHeaderToContext call.
			//
			// ReadFrame is safe to be called multiple times so it
			// won't break when it's called again later when we
			// actually start to read the message.
			if err := headerProtocol.ReadFrame(ctx); err != nil {
				return err
			}
			ctx = thrift.AddReadTHeaderToContext(ctx, headerProtocol.GetReadHeaders())
			ctx = thrift.SetWriteHeaderList(ctx, p.forwardHeaders)
		}

		ok, err := processor.Process(ctx, inputProtocol, outputProtocol)
		if errors.Is(err, thrift.ErrAbandonRequest) {
			return client.Close()
		}
		if errors.As(err, new(thrift.TTransportException)) && err != nil {
			return err
		}
		var tae thrift.TApplicationException
		if errors.As(err, &tae) && tae.TypeId() == thrift.UNKNOWN_METHOD {
			continue
		}
		if !ok {
			break
		}
	}
	return nil
}

// If err is actually EOF, return nil, otherwise return err as-is.
func treatEOFErrorsAsNil(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, io.EOF) {
		return nil
	}
	var te thrift.TTransportException
	if errors.As(err, &te) && te.TypeId() == thrift.END_OF_FILE {
		return nil
	}
	return err
}
