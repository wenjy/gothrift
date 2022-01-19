package pool

import (
	"sync/atomic"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

// Conn 封装了tSocket，增加了一些属性
type Conn struct {
	usedAt    int64 // atomic 记录使用的时间
	tSocket   *thrift.TSocket
	Inited    bool
	pooled    bool      // 是否在连接池里
	createdAt time.Time // 创建时间
}

// NewConn 创建一个新连接
func NewConn(tSocket *thrift.TSocket) *Conn {
	cn := &Conn{
		tSocket:   tSocket,
		createdAt: time.Now(),
	}
	cn.SetUsedAt(time.Now())
	return cn
}

// GetTSocket 获取TSocket
func (cn *Conn) GetTSocket() *thrift.TSocket {
	return cn.tSocket
}

// UsedAt 获取使用的时间
func (cn *Conn) UsedAt() time.Time {
	unix := atomic.LoadInt64(&cn.usedAt)
	return time.Unix(unix, 0)
}

// SetUsedAt 设置使用时间
func (cn *Conn) SetUsedAt(tm time.Time) {
	atomic.StoreInt64(&cn.usedAt, tm.Unix())
}

// Close 关闭连接
func (cn *Conn) Close() error {
	return cn.tSocket.Close()
}
