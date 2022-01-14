package client

import (
	"io"
	"strings"
)

// 连接池
var connPool *ConnPool

type TransPool struct {
	connPool *ConnPool
}

func NewTransPool(host, port string) *TransPool {
	return &TransPool{connPool: pool}
}

// 从连接池获取连接
func (t *TransPool) GetConn() (*Conn, error) {
	c, err := t.connPool.Get(defaultCtx)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// 把连接放入连接池
func (t *TransPool) PutConn(c *Conn, err error) {
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
func (t *TransPool) PoolStats() *Stats {
	return t.connPool.Stats()
}

func connIsClose(err error) bool {
	return err == io.EOF ||
		strings.Contains(err.Error(), "use of closed network connection") ||
		strings.Contains(err.Error(), "the mux has closed") ||
		strings.Contains(err.Error(), "connection reset by peer") ||
		strings.Contains(err.Error(), "broken pipe")
}
