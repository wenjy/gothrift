package client

import (
	"io"
	"strings"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/wenjy/gothrift/pool"
)

type TransPool struct {
	connPool *pool.ConnPool
}

func NewTransPool(opt *pool.Options) *TransPool {
	return &TransPool{connPool: pool.NewConnPool(opt)}
}

func (t *TransPool) Client(serviceName string, tConfig *thrift.TConfiguration) (rc *RetryClient, err error) {
	rc, err = NewRetryClient(t, serviceName, tConfig)
	return
}

// 从连接池获取连接
func (t *TransPool) GetConn() (*pool.Conn, error) {
	c, err := t.connPool.Get(defaultCtx)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// 把连接放入连接池
func (t *TransPool) PutConn(c *pool.Conn, err error) {
	// 已关闭的连接不再放回去
	if err != nil && connIsClose(err) {
		// 从总连接池删除
		t.connPool.Remove(defaultCtx, c)
		return
	}
	t.connPool.Put(defaultCtx, c)
}

// 关闭连接池
func (t *TransPool) ClosePool() error {
	return t.connPool.Close()
}

// 连接池状态
func (t *TransPool) PoolStats() *pool.Stats {
	return t.connPool.Stats()
}

func connIsClose(err error) bool {
	return err == io.EOF ||
		strings.Contains(err.Error(), "use of closed network connection") ||
		strings.Contains(err.Error(), "the mux has closed") ||
		strings.Contains(err.Error(), "connection reset by peer") ||
		strings.Contains(err.Error(), "broken pipe")
}
